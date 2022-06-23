package slack

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type UserPrefsCarrier struct {
	SlackResponse
	UserPrefs *UserPrefs `json:"prefs"`
}

// UserPrefs carries a bunch of user settings including some unknown types
type UserPrefs struct {
	UserColors       string `json:"user_colors,omitempty"`
	ColorNamesInList bool   `json:"color_names_in_list,omitempty"`
	// Keyboard UnknownType `json:"keyboard"`
	EmailAlerts                         string `json:"email_alerts,omitempty"`
	EmailAlertsSleepUntil               int    `json:"email_alerts_sleep_until,omitempty"`
	EmailTips                           bool   `json:"email_tips,omitempty"`
	EmailWeekly                         bool   `json:"email_weekly,omitempty"`
	EmailOffers                         bool   `json:"email_offers,omitempty"`
	EmailResearch                       bool   `json:"email_research,omitempty"`
	EmailDeveloper                      bool   `json:"email_developer,omitempty"`
	WelcomeMessageHidden                bool   `json:"welcome_message_hidden,omitempty"`
	SearchSort                          string `json:"search_sort,omitempty"`
	SearchFileSort                      string `json:"search_file_sort,omitempty"`
	SearchChannelSort                   string `json:"search_channel_sort,omitempty"`
	SearchPeopleSort                    string `json:"search_people_sort,omitempty"`
	ExpandInlineImages                  bool   `json:"expand_inline_images,omitempty"`
	ExpandInternalInlineImages          bool   `json:"expand_internal_inline_images,omitempty"`
	ExpandSnippets                      bool   `json:"expand_snippets,omitempty"`
	PostsFormattingGuide                bool   `json:"posts_formatting_guide,omitempty"`
	SeenWelcome2                        bool   `json:"seen_welcome_2,omitempty"`
	SeenSSBPrompt                       bool   `json:"seen_ssb_prompt,omitempty"`
	SpacesNewXpBannerDismissed          bool   `json:"spaces_new_xp_banner_dismissed,omitempty"`
	SearchOnlyMyChannels                bool   `json:"search_only_my_channels,omitempty"`
	SearchOnlyCurrentTeam               bool   `json:"search_only_current_team,omitempty"`
	SearchHideMyChannels                bool   `json:"search_hide_my_channels,omitempty"`
	SearchOnlyShowOnline                bool   `json:"search_only_show_online,omitempty"`
	SearchHideDeactivatedUsers          bool   `json:"search_hide_deactivated_users,omitempty"`
	EmojiMode                           string `json:"emoji_mode,omitempty"`
	EmojiUse                            string `json:"emoji_use,omitempty"`
	HasInvited                          bool   `json:"has_invited,omitempty"`
	HasUploaded                         bool   `json:"has_uploaded,omitempty"`
	HasCreatedChannel                   bool   `json:"has_created_channel,omitempty"`
	HasSearched                         bool   `json:"has_searched,omitempty"`
	SearchExcludeChannels               string `json:"search_exclude_channels,omitempty"`
	MessagesTheme                       string `json:"messages_theme,omitempty"`
	WebappSpellcheck                    bool   `json:"webapp_spellcheck,omitempty"`
	NoJoinedOverlays                    bool   `json:"no_joined_overlays,omitempty"`
	NoCreatedOverlays                   bool   `json:"no_created_overlays,omitempty"`
	DropboxEnabled                      bool   `json:"dropbox_enabled,omitempty"`
	SeenDomainInviteReminder            bool   `json:"seen_domain_invite_reminder,omitempty"`
	SeenMemberInviteReminder            bool   `json:"seen_member_invite_reminder,omitempty"`
	MuteSounds                          bool   `json:"mute_sounds,omitempty"`
	ArrowHistory                        bool   `json:"arrow_history,omitempty"`
	TabUIReturnSelects                  bool   `json:"tab_ui_return_selects,omitempty"`
	ObeyInlineImgLimit                  bool   `json:"obey_inline_img_limit,omitempty"`
	RequireAt                           bool   `json:"require_at,omitempty"`
	SsbSpaceWindow                      string `json:"ssb_space_window,omitempty"`
	MacSsbBounce                        string `json:"mac_ssb_bounce,omitempty"`
	MacSsbBullet                        bool   `json:"mac_ssb_bullet,omitempty"`
	ExpandNonMediaAttachments           bool   `json:"expand_non_media_attachments,omitempty"`
	ShowTyping                          bool   `json:"show_typing,omitempty"`
	PagekeysHandled                     bool   `json:"pagekeys_handled,omitempty"`
	LastSnippetType                     string `json:"last_snippet_type,omitempty"`
	DisplayRealNamesOverride            int    `json:"display_real_names_override,omitempty"`
	DisplayDisplayNames                 bool   `json:"display_display_names,omitempty"`
	Time24                              bool   `json:"time24,omitempty"`
	EnterIsSpecialInTbt                 bool   `json:"enter_is_special_in_tbt,omitempty"`
	MsgInputSendBtn                     bool   `json:"msg_input_send_btn,omitempty"`
	MsgInputSendBtnAutoSet              bool   `json:"msg_input_send_btn_auto_set,omitempty"`
	MsgInputStickyComposer              bool   `json:"msg_input_sticky_composer,omitempty"`
	GraphicEmoticons                    bool   `json:"graphic_emoticons,omitempty"`
	ConvertEmoticons                    bool   `json:"convert_emoticons,omitempty"`
	SsEmojis                            bool   `json:"ss_emojis,omitempty"`
	SeenOnboardingStart                 bool   `json:"seen_onboarding_start,omitempty"`
	OnboardingCancelled                 bool   `json:"onboarding_cancelled,omitempty"`
	SeenOnboardingSlackbotConversation  bool   `json:"seen_onboarding_slackbot_conversation,omitempty"`
	SeenOnboardingChannels              bool   `json:"seen_onboarding_channels,omitempty"`
	SeenOnboardingDirectMessages        bool   `json:"seen_onboarding_direct_messages,omitempty"`
	SeenOnboardingInvites               bool   `json:"seen_onboarding_invites,omitempty"`
	SeenOnboardingSearch                bool   `json:"seen_onboarding_search,omitempty"`
	SeenOnboardingRecentMentions        bool   `json:"seen_onboarding_recent_mentions,omitempty"`
	SeenOnboardingStarredItems          bool   `json:"seen_onboarding_starred_items,omitempty"`
	SeenOnboardingPrivateGroups         bool   `json:"seen_onboarding_private_groups,omitempty"`
	SeenOnboardingBanner                bool   `json:"seen_onboarding_banner,omitempty"`
	OnboardingSlackbotConversationStep  int    `json:"onboarding_slackbot_conversation_step,omitempty"`
	SetTzAutomatically                  bool   `json:"set_tz_automatically,omitempty"`
	SuppressLinkWarning                 bool   `json:"suppress_link_warning,omitempty"`
	DndEnabled                          bool   `json:"dnd_enabled,omitempty"`
	DndStartHour                        string `json:"dnd_start_hour,omitempty"`
	DndEndHour                          string `json:"dnd_end_hour,omitempty"`
	DndBeforeMonday                     string `json:"dnd_before_monday,omitempty"`
	DndAfterMonday                      string `json:"dnd_after_monday,omitempty"`
	DndEnabledMonday                    string `json:"dnd_enabled_monday,omitempty"`
	DndBeforeTuesday                    string `json:"dnd_before_tuesday,omitempty"`
	DndAfterTuesday                     string `json:"dnd_after_tuesday,omitempty"`
	DndEnabledTuesday                   string `json:"dnd_enabled_tuesday,omitempty"`
	DndBeforeWednesday                  string `json:"dnd_before_wednesday,omitempty"`
	DndAfterWednesday                   string `json:"dnd_after_wednesday,omitempty"`
	DndEnabledWednesday                 string `json:"dnd_enabled_wednesday,omitempty"`
	DndBeforeThursday                   string `json:"dnd_before_thursday,omitempty"`
	DndAfterThursday                    string `json:"dnd_after_thursday,omitempty"`
	DndEnabledThursday                  string `json:"dnd_enabled_thursday,omitempty"`
	DndBeforeFriday                     string `json:"dnd_before_friday,omitempty"`
	DndAfterFriday                      string `json:"dnd_after_friday,omitempty"`
	DndEnabledFriday                    string `json:"dnd_enabled_friday,omitempty"`
	DndBeforeSaturday                   string `json:"dnd_before_saturday,omitempty"`
	DndAfterSaturday                    string `json:"dnd_after_saturday,omitempty"`
	DndEnabledSaturday                  string `json:"dnd_enabled_saturday,omitempty"`
	DndBeforeSunday                     string `json:"dnd_before_sunday,omitempty"`
	DndAfterSunday                      string `json:"dnd_after_sunday,omitempty"`
	DndEnabledSunday                    string `json:"dnd_enabled_sunday,omitempty"`
	DndDays                             string `json:"dnd_days,omitempty"`
	DndCustomNewBadgeSeen               bool   `json:"dnd_custom_new_badge_seen,omitempty"`
	DndNotificationScheduleNewBadgeSeen bool   `json:"dnd_notification_schedule_new_badge_seen,omitempty"`
	// UnreadCollapsedChannels      unknownType                  `json:"unread_collapsed_channels,omitempty"`
	SidebarBehavior          string `json:"sidebar_behavior,omitempty"`
	ChannelSort              string `json:"channel_sort,omitempty"`
	SeparatePrivateChannels  bool   `json:"separate_private_channels,omitempty"`
	SeparateSharedChannels   bool   `json:"separate_shared_channels,omitempty"`
	SidebarTheme             string `json:"sidebar_theme,omitempty"`
	SidebarThemeCustomValues string `json:"sidebar_theme_custom_values,omitempty"`
	NoInvitesWidgetInSidebar bool   `json:"no_invites_widget_in_sidebar,omitempty"`
	NoOmniboxInChannels      bool   `json:"no_omnibox_in_channels,omitempty"`

	KKeyOmniboxAutoHideCount       int    `json:"k_key_omnibox_auto_hide_count,omitempty"`
	ShowSidebarQuickswitcherButton bool   `json:"show_sidebar_quickswitcher_button,omitempty"`
	EntOrgWideChannelsSidebar      bool   `json:"ent_org_wide_channels_sidebar,omitempty"`
	MarkMsgsReadImmediately        bool   `json:"mark_msgs_read_immediately,omitempty"`
	StartScrollAtOldest            bool   `json:"start_scroll_at_oldest,omitempty"`
	SnippetEditorWrapLongLines     bool   `json:"snippet_editor_wrap_long_lines,omitempty"`
	LsDisabled                     bool   `json:"ls_disabled,omitempty"`
	FKeySearch                     bool   `json:"f_key_search,omitempty"`
	KKeyOmnibox                    bool   `json:"k_key_omnibox,omitempty"`
	PromptedForEmailDisabling      bool   `json:"prompted_for_email_disabling,omitempty"`
	NoMacelectronBanner            bool   `json:"no_macelectron_banner,omitempty"`
	NoMacssb1Banner                bool   `json:"no_macssb1_banner,omitempty"`
	NoMacssb2Banner                bool   `json:"no_macssb2_banner,omitempty"`
	NoWinssb1Banner                bool   `json:"no_winssb1_banner,omitempty"`
	HideUserGroupInfoPane          bool   `json:"hide_user_group_info_pane,omitempty"`
	MentionsExcludeAtUserGroups    bool   `json:"mentions_exclude_at_user_groups,omitempty"`
	MentionsExcludeReactions       bool   `json:"mentions_exclude_reactions,omitempty"`
	PrivacyPolicySeen              bool   `json:"privacy_policy_seen,omitempty"`
	EnterpriseMigrationSeen        bool   `json:"enterprise_migration_seen,omitempty"`
	LastTosAcknowledged            string `json:"last_tos_acknowledged,omitempty"`
	SearchExcludeBots              bool   `json:"search_exclude_bots,omitempty"`
	LoadLato2                      bool   `json:"load_lato_2,omitempty"`
	FullerTimestamps               bool   `json:"fuller_timestamps,omitempty"`
	LastSeenAtChannelWarning       int    `json:"last_seen_at_channel_warning,omitempty"`
	EmojiAutocompleteBig           bool   `json:"emoji_autocomplete_big,omitempty"`
	TwoFactorAuthEnabled           bool   `json:"two_factor_auth_enabled,omitempty"`
	// TwoFactorType                         unknownType    `json:"two_factor_type,omitempty"`
	// TwoFactorBackupType                   unknownType    `json:"two_factor_backup_type,omitempty"`
	HideHexSwatch          bool   `json:"hide_hex_swatch,omitempty"`
	ShowJumperScores       bool   `json:"show_jumper_scores,omitempty"`
	EnterpriseMdmCustomMsg string `json:"enterprise_mdm_custom_msg,omitempty"`
	// EnterpriseExcludedAppTeams                 unknownType    `json:"enterprise_excluded_app_teams,omitempty"`
	ClientLogsPri             string `json:"client_logs_pri,omitempty"`
	FlannelServerPool         string `json:"flannel_server_pool,omitempty"`
	MentionsExcludeAtChannels bool   `json:"mentions_exclude_at_channels,omitempty"`
	ConfirmClearAllUnreads    bool   `json:"confirm_clear_all_unreads,omitempty"`
	ConfirmUserMarkedAway     bool   `json:"confirm_user_marked_away,omitempty"`
	BoxEnabled                bool   `json:"box_enabled,omitempty"`
	SeenSingleEmojiMsg        bool   `json:"seen_single_emoji_msg,omitempty"`
	ConfirmShCallStart        bool   `json:"confirm_sh_call_start,omitempty"`
	PreferredSkinTone         string `json:"preferred_skin_tone,omitempty"`
	ShowAllSkinTones          bool   `json:"show_all_skin_tones,omitempty"`
	WhatsNewRead              int    `json:"whats_new_read,omitempty"`
	// FrecencyJumper                           unknownType      `json:"frecency_jumper,omitempty"`
	FrecencyEntJumper                       string `json:"frecency_ent_jumper,omitempty"`
	FrecencyEntJumperBackup                 string `json:"frecency_ent_jumper_backup,omitempty"`
	Jumbomoji                               bool   `json:"jumbomoji,omitempty"`
	NewxpSeenLastMessage                    int    `json:"newxp_seen_last_message,omitempty"`
	ShowMemoryInstrument                    bool   `json:"show_memory_instrument,omitempty"`
	EnableUnreadView                        bool   `json:"enable_unread_view,omitempty"`
	SeenUnreadViewCoachmark                 bool   `json:"seen_unread_view_coachmark,omitempty"`
	EnableReactEmojiPicker                  bool   `json:"enable_react_emoji_picker,omitempty"`
	SeenCustomStatusBadge                   bool   `json:"seen_custom_status_badge,omitempty"`
	SeenCustomStatusCallout                 bool   `json:"seen_custom_status_callout,omitempty"`
	SeenCustomStatusExpirationBadge         bool   `json:"seen_custom_status_expiration_badge,omitempty"`
	UsedCustomStatusKbShortcut              bool   `json:"used_custom_status_kb_shortcut,omitempty"`
	SeenGuestAdminSlackbotAnnouncement      bool   `json:"seen_guest_admin_slackbot_announcement,omitempty"`
	SeenThreadsNotificationBanner           bool   `json:"seen_threads_notification_banner,omitempty"`
	SeenNameTaggingCoachmark                bool   `json:"seen_name_tagging_coachmark,omitempty"`
	AllUnreadsSortOrder                     string `json:"all_unreads_sort_order,omitempty"`
	Locale                                  string `json:"locale,omitempty"`
	SeenIntlChannelNamesCoachmark           bool   `json:"seen_intl_channel_names_coachmark,omitempty"`
	SeenP2LocaleChangeMessage               int    `json:"seen_p2_locale_change_message,omitempty"`
	SeenLocaleChangeMessage                 int    `json:"seen_locale_change_message,omitempty"`
	SeenJapaneseLocaleChangeMessage         bool   `json:"seen_japanese_locale_change_message,omitempty"`
	SeenSharedChannelsCoachmark             bool   `json:"seen_shared_channels_coachmark,omitempty"`
	SeenSharedChannelsOptInChangeMessage    bool   `json:"seen_shared_channels_opt_in_change_message,omitempty"`
	HasRecentlySharedaChannel               bool   `json:"has_recently_shared_a_channel,omitempty"`
	SeenChannelBrowserAdminCoachmark        bool   `json:"seen_channel_browser_admin_coachmark,omitempty"`
	SeenAdministrationMenu                  bool   `json:"seen_administration_menu,omitempty"`
	SeenDraftsSectionCoachmark              bool   `json:"seen_drafts_section_coachmark,omitempty"`
	SeenEmojiUpdateOverlayCoachmark         bool   `json:"seen_emoji_update_overlay_coachmark,omitempty"`
	SeenSonicDeluxeToast                    int    `json:"seen_sonic_deluxe_toast,omitempty"`
	SeenWysiwygDeluxeToast                  bool   `json:"seen_wysiwyg_deluxe_toast,omitempty"`
	SeenMarkdownPasteToast                  int    `json:"seen_markdown_paste_toast,omitempty"`
	SeenMarkdownPasteShortcut               int    `json:"seen_markdown_paste_shortcut,omitempty"`
	SeenIaEducation                         bool   `json:"seen_ia_education,omitempty"`
	PlainTextMode                           bool   `json:"plain_text_mode,omitempty"`
	ShowSharedChannelsEducationBanner       bool   `json:"show_shared_channels_education_banner,omitempty"`
	AllowCallsToSetCurrentStatus            bool   `json:"allow_calls_to_set_current_status,omitempty"`
	InInteractiveMasMigrationFlow           bool   `json:"in_interactive_mas_migration_flow,omitempty"`
	SunsetInteractiveMessageViews           int    `json:"sunset_interactive_message_views,omitempty"`
	ShdepPromoCodeSubmitted                 bool   `json:"shdep_promo_code_submitted,omitempty"`
	SeenShdepSlackbotMessage                bool   `json:"seen_shdep_slackbot_message,omitempty"`
	SeenCallsInteractiveCoachmark           bool   `json:"seen_calls_interactive_coachmark,omitempty"`
	AllowCmdTabIss                          bool   `json:"allow_cmd_tab_iss,omitempty"`
	SeenWorkflowBuilderDeluxeToast          bool   `json:"seen_workflow_builder_deluxe_toast,omitempty"`
	WorkflowBuilderIntroModalClickedThrough bool   `json:"workflow_builder_intro_modal_clicked_through,omitempty"`
	// WorkflowBuilderCoachmarks                    unknownType  `json:"workflow_builder_coachmarks,omitempty"`
	SeenGdriveCoachmark                            bool   `json:"seen_gdrive_coachmark,omitempty"`
	OverloadedMessageEnabled                       bool   `json:"overloaded_message_enabled,omitempty"`
	SeenHighlightsCoachmark                        bool   `json:"seen_highlights_coachmark,omitempty"`
	SeenHighlightsArrowsCoachmark                  bool   `json:"seen_highlights_arrows_coachmark,omitempty"`
	SeenHighlightsWarmWelcome                      bool   `json:"seen_highlights_warm_welcome,omitempty"`
	SeenNewSearchUi                                bool   `json:"seen_new_search_ui,omitempty"`
	SeenChannelSearch                              bool   `json:"seen_channel_search,omitempty"`
	SeenPeopleSearch                               bool   `json:"seen_people_search,omitempty"`
	SeenPeopleSearchCount                          int    `json:"seen_people_search_count,omitempty"`
	DismissedScrollSearchTooltipCount              int    `json:"dismissed_scroll_search_tooltip_count,omitempty"`
	LastDismissedScrollSearchTooltipTimestamp      int    `json:"last_dismissed_scroll_search_tooltip_timestamp,omitempty"`
	HasUsedQuickswitcherShortcut                   bool   `json:"has_used_quickswitcher_shortcut,omitempty"`
	SeenQuickswitcherShortcutTipCount              int    `json:"seen_quickswitcher_shortcut_tip_count,omitempty"`
	BrowsersDismissedChannelsLowResultsEducation   bool   `json:"browsers_dismissed_channels_low_results_education,omitempty"`
	BrowsersSeenInitialChannelsEducation           bool   `json:"browsers_seen_initial_channels_education,omitempty"`
	BrowsersDismissedPeopleLowResultsEducation     bool   `json:"browsers_dismissed_people_low_results_education,omitempty"`
	BrowsersSeenInitialPeopleEducation             bool   `json:"browsers_seen_initial_people_education,omitempty"`
	BrowsersDismissedUserGroupsLowResultsEducation bool   `json:"browsers_dismissed_user_groups_low_results_education,omitempty"`
	BrowsersSeenInitialUserGroupsEducation         bool   `json:"browsers_seen_initial_user_groups_education,omitempty"`
	BrowsersDismissedFilesLowResultsEducation      bool   `json:"browsers_dismissed_files_low_results_education,omitempty"`
	BrowsersSeenInitialFilesEducation              bool   `json:"browsers_seen_initial_files_education,omitempty"`
	A11yAnimations                                 bool   `json:"a11y_animations,omitempty"`
	SeenKeyboardShortcutsCoachmark                 bool   `json:"seen_keyboard_shortcuts_coachmark,omitempty"`
	NeedsInitialPasswordSet                        bool   `json:"needs_initial_password_set,omitempty"`
	LessonsEnabled                                 bool   `json:"lessons_enabled,omitempty"`
	TractorEnabled                                 bool   `json:"tractor_enabled,omitempty"`
	TractorExperimentGroup                         string `json:"tractor_experiment_group,omitempty"`
	OpenedSlackbotDm                               bool   `json:"opened_slackbot_dm,omitempty"`
	NewxpSuggestedChannels                         string `json:"newxp_suggested_channels,omitempty"`
	OnboardingComplete                             bool   `json:"onboarding_complete,omitempty"`
	WelcomePlaceState                              string `json:"welcome_place_state,omitempty"`
	// OnboardingRoleApps  unknownType `json:"onboarding_role_apps,omitempty"`
	HasReceivedThreadedMessage        bool   `json:"has_received_threaded_message,omitempty"`
	SendYourFirstMessageBannerEnabled bool   `json:"send_your_first_message_banner_enabled,omitempty"`
	WhocanseethisDmMpdmBadge          bool   `json:"whocanseethis_dm_mpdm_badge,omitempty"`
	HighlightWords                    string `json:"highlight_words,omitempty"`
	ThreadsEverything                 bool   `json:"threads_everything,omitempty"`
	NoTextInNotifications             bool   `json:"no_text_in_notifications,omitempty"`
	PushShowPreview                   bool   `json:"push_show_preview,omitempty"`
	GrowlsEnabled                     bool   `json:"growls_enabled,omitempty"`
	AllChannelsLoud                   bool   `json:"all_channels_loud,omitempty"`
	PushDmAlert                       bool   `json:"push_dm_alert,omitempty"`
	PushMentionAlert                  bool   `json:"push_mention_alert,omitempty"`
	PushEverything                    bool   `json:"push_everything,omitempty"`
	PushIdleWait                      int    `json:"push_idle_wait,omitempty"`
	PushSound                         string `json:"push_sound,omitempty"`
	NewMsgSnd                         string `json:"new_msg_snd,omitempty"`
	PushLoudChannels                  string `json:"push_loud_channels,omitempty"`
	PushMentionChannels               string `json:"push_mention_channels,omitempty"`
	PushLoudChannelsSet               string `json:"push_loud_channels_set,omitempty"`
	LoudChannels                      string `json:"loud_channels,omitempty"`
	NeverChannels                     string `json:"never_channels,omitempty"`
	LoudChannelsSet                   string `json:"loud_channels_set,omitempty"`
	AtChannelSuppressedChannels       string `json:"at_channel_suppressed_channels,omitempty"`
	PushAtChannelSuppressedChannels   string `json:"push_at_channel_suppressed_channels,omitempty"`
	MutedChannels                     string `json:"muted_channels,omitempty"`
	// AllNotificationsPrefs                  unknownType `json:"all_notifications_prefs,omitempty"`
	GrowthMsgLimitApproachingCtaCount     int `json:"growth_msg_limit_approaching_cta_count,omitempty"`
	GrowthMsgLimitApproachingCtaTs        int `json:"growth_msg_limit_approaching_cta_ts,omitempty"`
	GrowthMsgLimitReachedCtaCount         int `json:"growth_msg_limit_reached_cta_count,omitempty"`
	GrowthMsgLimitReachedCtaLastTs        int `json:"growth_msg_limit_reached_cta_last_ts,omitempty"`
	GrowthMsgLimitLongReachedCtaCount     int `json:"growth_msg_limit_long_reached_cta_count,omitempty"`
	GrowthMsgLimitLongReachedCtaLastTs    int `json:"growth_msg_limit_long_reached_cta_last_ts,omitempty"`
	GrowthMsgLimitSixtyDayBannerCtaCount  int `json:"growth_msg_limit_sixty_day_banner_cta_count,omitempty"`
	GrowthMsgLimitSixtyDayBannerCtaLastTs int `json:"growth_msg_limit_sixty_day_banner_cta_last_ts,omitempty"`
	// GrowthAllBannersPrefs unknownType `json:"growth_all_banners_prefs,omitempty"`
	AnalyticsUpsellCoachmarkSeen bool `json:"analytics_upsell_coachmark_seen,omitempty"`
	SeenAppSpaceCoachmark        bool `json:"seen_app_space_coachmark,omitempty"`
	SeenAppSpaceTutorial         bool `json:"seen_app_space_tutorial,omitempty"`
	DismissedAppLauncherWelcome  bool `json:"dismissed_app_launcher_welcome,omitempty"`
	DismissedAppLauncherLimit    bool `json:"dismissed_app_launcher_limit,omitempty"`
	Purchaser                    bool `json:"purchaser,omitempty"`
	ShowEntOnboarding            bool `json:"show_ent_onboarding,omitempty"`
	FoldersEnabled               bool `json:"folders_enabled,omitempty"`
	// FolderData unknownType `json:"folder_data,omitempty"`
	SeenCorporateExportAlert               bool   `json:"seen_corporate_export_alert,omitempty"`
	ShowAutocompleteHelp                   int    `json:"show_autocomplete_help,omitempty"`
	DeprecationToastLastSeen               int    `json:"deprecation_toast_last_seen,omitempty"`
	DeprecationModalLastSeen               int    `json:"deprecation_modal_last_seen,omitempty"`
	Iap1Lab                                int    `json:"iap1_lab,omitempty"`
	IaTopNavTheme                          string `json:"ia_top_nav_theme,omitempty"`
	IaPlatformActionsLab                   int    `json:"ia_platform_actions_lab,omitempty"`
	ActivityView                           string `json:"activity_view,omitempty"`
	FailoverProxyCheckCompleted            int    `json:"failover_proxy_check_completed,omitempty"`
	EdgeUploadProxyCheckCompleted          int    `json:"edge_upload_proxy_check_completed,omitempty"`
	AppSubdomainCheckCompleted             int    `json:"app_subdomain_check_completed,omitempty"`
	AddAppsPromptDismissed                 bool   `json:"add_apps_prompt_dismissed,omitempty"`
	AddChannelPromptDismissed              bool   `json:"add_channel_prompt_dismissed,omitempty"`
	ChannelSidebarHideInvite               bool   `json:"channel_sidebar_hide_invite,omitempty"`
	InProdSurveysEnabled                   bool   `json:"in_prod_surveys_enabled,omitempty"`
	DismissedInstalledAppDmSuggestions     string `json:"dismissed_installed_app_dm_suggestions,omitempty"`
	SeenContextualMessageShortcutsModal    bool   `json:"seen_contextual_message_shortcuts_modal,omitempty"`
	SeenMessageNavigationEducationalToast  bool   `json:"seen_message_navigation_educational_toast,omitempty"`
	ContextualMessageShortcutsModalWasSeen bool   `json:"contextual_message_shortcuts_modal_was_seen,omitempty"`
	MessageNavigationToastWasSeen          bool   `json:"message_navigation_toast_was_seen,omitempty"`
	UpToBrowseKbShortcut                   bool   `json:"up_to_browse_kb_shortcut,omitempty"`
	ChannelSections                        string `json:"channel_sections,omitempty"`
	TZ                                     string `json:"tz,omitempty"`
}

func (api *Client) GetUserPrefs() (*UserPrefsCarrier, error) {
	response := UserPrefsCarrier{}

	err := api.getMethod(context.Background(), "users.prefs.get", api.token, url.Values{}, &response)
	if err != nil {
		return nil, err
	}

	return &response, response.Err()
}

func (api *Client) MuteChat(channelID string) (*UserPrefsCarrier, error) {
	prefs, err := api.GetUserPrefs()
	if err != nil {
		return nil, err
	}
	chnls := strings.Split(prefs.UserPrefs.MutedChannels, ",")
	for _, chn := range chnls {
		if chn == channelID {
			return nil, nil // noop
		}
	}
	newChnls := prefs.UserPrefs.MutedChannels + "," + channelID
	values := url.Values{"token": {api.token}, "muted_channels": {newChnls}, "reason": {"update-muted-channels"}}
	response := UserPrefsCarrier{}

	err = api.postMethod(context.Background(), "users.prefs.set", values, &response)
	if err != nil {
		return nil, err
	}

	return &response, response.Err()
}

func (api *Client) UnMuteChat(channelID string) (*UserPrefsCarrier, error) {
	prefs, err := api.GetUserPrefs()
	if err != nil {
		return nil, err
	}
	chnls := strings.Split(prefs.UserPrefs.MutedChannels, ",")
	newChnls := make([]string, len(chnls)-1)
	for i, chn := range chnls {
		if chn == channelID {
			return nil, nil // noop
		}
		newChnls[i] = chn
	}
	values := url.Values{"token": {api.token}, "muted_channels": {strings.Join(newChnls, ",")}, "reason": {"update-muted-channels"}}
	response := UserPrefsCarrier{}

	err = api.postMethod(context.Background(), "users.prefs.set", values, &response)
	if err != nil {
		return nil, err
	}

	return &response, response.Err()
}

// UserDetails contains user details coming in the initial response from StartRTM
type UserDetails struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Created        JSONTime  `json:"created"`
	ManualPresence string    `json:"manual_presence"`
	Prefs          UserPrefs `json:"prefs"`
}

// JSONTime exists so that we can have a String method converting the date
type JSONTime int64

// String converts the unix timestamp into a string
func (t JSONTime) String() string {
	tm := t.Time()
	return fmt.Sprintf("\"%s\"", tm.Format("Mon Jan _2"))
}

// Time returns a `time.Time` representation of this value.
func (t JSONTime) Time() time.Time {
	return time.Unix(int64(t), 0)
}

// UnmarshalJSON will unmarshal both string and int JSON values
func (t *JSONTime) UnmarshalJSON(buf []byte) error {
	s := bytes.Trim(buf, `"`)

	v, err := strconv.Atoi(string(s))
	if err != nil {
		return err
	}

	*t = JSONTime(int64(v))
	return nil
}

// Team contains details about a team
type Team struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Domain string `json:"domain"`
}

// Icons XXX: needs further investigation
type Icons struct {
	Image36 string `json:"image_36,omitempty"`
	Image48 string `json:"image_48,omitempty"`
	Image72 string `json:"image_72,omitempty"`
}

// Info contains various details about the authenticated user and team.
// It is returned by StartRTM or included in the "ConnectedEvent" RTM event.
type Info struct {
	URL  string       `json:"url,omitempty"`
	User *UserDetails `json:"self,omitempty"`
	Team *Team        `json:"team,omitempty"`
}

type infoResponseFull struct {
	Info
	SlackResponse
}

// GetBotByID is deprecated and returns nil
func (info Info) GetBotByID(botID string) *Bot {
	return nil
}

// GetUserByID is deprecated and returns nil
func (info Info) GetUserByID(userID string) *User {
	return nil
}

// GetChannelByID is deprecated and returns nil
func (info Info) GetChannelByID(channelID string) *Channel {
	return nil
}

// GetGroupByID is deprecated and returns nil
func (info Info) GetGroupByID(groupID string) *Group {
	return nil
}

// GetIMByID is deprecated and returns nil
func (info Info) GetIMByID(imID string) *IM {
	return nil
}
