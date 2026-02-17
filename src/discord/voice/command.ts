import {
  ChannelType as CarbonChannelType,
  Command,
  CommandWithSubcommands,
  type CommandInteraction,
} from "@buape/carbon";
import {
  ApplicationCommandOptionType,
  ChannelType as DiscordChannelType,
} from "discord-api-types/v10";
import type { OpenClawConfig } from "../../config/config.js";
import type { DiscordAccountConfig } from "../../config/types.js";
import type { DiscordVoiceManager } from "./manager.js";
import { resolveCommandAuthorizedFromAuthorizers } from "../../channels/command-gating.js";
import {
  allowListMatches,
  isDiscordGroupAllowedByPolicy,
  normalizeDiscordAllowList,
  normalizeDiscordSlug,
  resolveDiscordChannelConfigWithFallback,
  resolveDiscordGuildEntry,
  resolveDiscordMemberAccessState,
} from "../monitor/allow-list.js";
import { resolveDiscordChannelInfo } from "../monitor/message-utils.js";
import { resolveDiscordSenderIdentity } from "../monitor/sender-identity.js";
import { resolveDiscordThreadParentInfo } from "../monitor/threading.js";

const VOICE_CHANNEL_TYPES: DiscordChannelType[] = [
  DiscordChannelType.GuildVoice,
  DiscordChannelType.GuildStageVoice,
];

type VoiceCommandContext = {
  cfg: OpenClawConfig;
  discordConfig: DiscordAccountConfig;
  accountId: string;
  groupPolicy: "open" | "disabled" | "allowlist";
  useAccessGroups: boolean;
  getManager: () => DiscordVoiceManager | null;
  ephemeralDefault: boolean;
};

async function authorizeVoiceCommand(
  interaction: CommandInteraction,
  params: VoiceCommandContext,
): Promise<{ ok: boolean; message?: string; guildId?: string }> {
  const channel = interaction.channel;
  if (!interaction.guild) {
    return { ok: false, message: "Voice commands are only available in guilds." };
  }
  const user = interaction.user;
  if (!user) {
    return { ok: false, message: "Unable to resolve command user." };
  }

  const channelId = channel?.id ?? "";
  const channelName = channel && "name" in channel ? (channel.name as string) : undefined;
  const channelSlug = channelName ? normalizeDiscordSlug(channelName) : "";
  const channelInfo = channelId
    ? await resolveDiscordChannelInfo(interaction.client, channelId)
    : null;
  const isThreadChannel =
    channelInfo?.type === CarbonChannelType.PublicThread ||
    channelInfo?.type === CarbonChannelType.PrivateThread ||
    channelInfo?.type === CarbonChannelType.AnnouncementThread;
  let parentId: string | undefined;
  let parentName: string | undefined;
  let parentSlug: string | undefined;
  if (isThreadChannel && channelId) {
    const parentInfo = await resolveDiscordThreadParentInfo({
      client: interaction.client,
      threadChannel: {
        id: channelId,
        name: channelName,
        parentId:
          "parentId" in (channel ?? {})
            ? ((channel as { parentId?: string }).parentId ?? undefined)
            : undefined,
        parent: undefined,
      },
      channelInfo,
    });
    parentId = parentInfo.id;
    parentName = parentInfo.name;
    parentSlug = parentName ? normalizeDiscordSlug(parentName) : undefined;
  }

  const guildInfo = resolveDiscordGuildEntry({
    guild: interaction.guild ?? undefined,
    guildEntries: params.discordConfig.guilds,
  });

  const channelConfig = channelId
    ? resolveDiscordChannelConfigWithFallback({
        guildInfo,
        channelId,
        channelName,
        channelSlug,
        parentId,
        parentName,
        parentSlug,
        scope: isThreadChannel ? "thread" : "channel",
      })
    : null;

  if (channelConfig?.enabled === false) {
    return { ok: false, message: "This channel is disabled." };
  }

  const channelAllowlistConfigured =
    Boolean(guildInfo?.channels) && Object.keys(guildInfo?.channels ?? {}).length > 0;
  const channelAllowed = channelConfig?.allowed !== false;
  if (
    !isDiscordGroupAllowedByPolicy({
      groupPolicy: params.groupPolicy,
      guildAllowlisted: Boolean(guildInfo),
      channelAllowlistConfigured,
      channelAllowed,
    }) ||
    channelConfig?.allowed === false
  ) {
    return { ok: false, message: "This channel is not allowlisted for commands." };
  }

  const memberRoleIds = Array.isArray(interaction.rawData.member?.roles)
    ? interaction.rawData.member.roles.map((roleId: string) => String(roleId))
    : [];
  const sender = resolveDiscordSenderIdentity({ author: user, member: interaction.rawData.member });

  const { hasAccessRestrictions, memberAllowed } = resolveDiscordMemberAccessState({
    channelConfig,
    guildInfo,
    memberRoleIds,
    sender,
  });

  const ownerAllowList = normalizeDiscordAllowList(
    params.discordConfig.allowFrom ?? params.discordConfig.dm?.allowFrom ?? [],
    ["discord:", "user:", "pk:"],
  );
  const ownerOk = ownerAllowList
    ? allowListMatches(ownerAllowList, {
        id: sender.id,
        name: sender.name,
        tag: sender.tag,
      })
    : false;

  const authorizers = params.useAccessGroups
    ? [
        { configured: ownerAllowList != null, allowed: ownerOk },
        { configured: hasAccessRestrictions, allowed: memberAllowed },
      ]
    : [{ configured: hasAccessRestrictions, allowed: memberAllowed }];

  const commandAuthorized = resolveCommandAuthorizedFromAuthorizers({
    useAccessGroups: params.useAccessGroups,
    authorizers,
    modeWhenAccessGroupsOff: "configured",
  });

  if (!commandAuthorized) {
    return { ok: false, message: "You are not authorized to use this command." };
  }

  return { ok: true, guildId: interaction.guild.id };
}

export function createDiscordVoiceCommand(params: VoiceCommandContext): CommandWithSubcommands {
  class JoinCommand extends Command {
    name = "join";
    description = "Join a voice channel";
    defer = true;
    ephemeral = params.ephemeralDefault;
    options = [
      {
        name: "channel",
        description: "Voice channel to join",
        type: ApplicationCommandOptionType.Channel,
        required: true,
        channel_types: VOICE_CHANNEL_TYPES,
      },
    ];

    async run(interaction: CommandInteraction) {
      const access = await authorizeVoiceCommand(interaction, params);
      if (!access.ok) {
        await interaction.reply({ content: access.message ?? "Not authorized.", ephemeral: true });
        return;
      }
      const channel = await interaction.options.getChannel("channel", true);
      if (!channel || !("id" in channel)) {
        await interaction.reply({ content: "Voice channel not found.", ephemeral: true });
        return;
      }
      if (!isVoiceChannelType(channel.type)) {
        await interaction.reply({ content: "That is not a voice channel.", ephemeral: true });
        return;
      }
      const guildId = access.guildId ?? ("guildId" in channel ? channel.guildId : undefined);
      if (!guildId) {
        await interaction.reply({
          content: "Unable to resolve guild for this voice channel.",
          ephemeral: true,
        });
        return;
      }

      const manager = params.getManager();
      if (!manager) {
        await interaction.reply({
          content: "Voice manager is not available yet.",
          ephemeral: true,
        });
        return;
      }

      const result = await manager.join({ guildId, channelId: channel.id });
      await interaction.reply({ content: result.message, ephemeral: true });
    }
  }

  class LeaveCommand extends Command {
    name = "leave";
    description = "Leave the current voice channel";
    defer = true;
    ephemeral = params.ephemeralDefault;

    async run(interaction: CommandInteraction) {
      const access = await authorizeVoiceCommand(interaction, params);
      if (!access.ok) {
        await interaction.reply({ content: access.message ?? "Not authorized.", ephemeral: true });
        return;
      }
      const guildId = access.guildId;
      if (!guildId) {
        await interaction.reply({
          content: "Unable to resolve guild for this command.",
          ephemeral: true,
        });
        return;
      }
      const manager = params.getManager();
      if (!manager) {
        await interaction.reply({
          content: "Voice manager is not available yet.",
          ephemeral: true,
        });
        return;
      }
      const result = await manager.leave({ guildId });
      await interaction.reply({ content: result.message, ephemeral: true });
    }
  }

  class StatusCommand extends Command {
    name = "status";
    description = "Show active voice sessions";
    defer = true;
    ephemeral = params.ephemeralDefault;

    async run(interaction: CommandInteraction) {
      const access = await authorizeVoiceCommand(interaction, params);
      if (!access.ok) {
        await interaction.reply({ content: access.message ?? "Not authorized.", ephemeral: true });
        return;
      }
      const manager = params.getManager();
      if (!manager) {
        await interaction.reply({
          content: "Voice manager is not available yet.",
          ephemeral: true,
        });
        return;
      }
      const sessions = manager.status();
      if (sessions.length === 0) {
        await interaction.reply({ content: "No active voice sessions.", ephemeral: true });
        return;
      }
      const lines = sessions.map((entry) => `• <#${entry.channelId}> (guild ${entry.guildId})`);
      await interaction.reply({ content: lines.join("\n"), ephemeral: true });
    }
  }

  return new (class extends CommandWithSubcommands {
    name = "vc";
    description = "Voice channel controls";
    subcommands = [new JoinCommand(), new LeaveCommand(), new StatusCommand()];
  })();
}

function isVoiceChannelType(type: CarbonChannelType) {
  return type === CarbonChannelType.GuildVoice || type === CarbonChannelType.GuildStageVoice;
}
