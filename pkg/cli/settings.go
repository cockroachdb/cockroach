package cli

import "github.com/cockroachdb/cockroach/pkg/settings"

const (
	baseDebugZipSettingName                            = "debug.zip"
	DebugZipSensitiveFieldsRedactionEnabledSettingName = baseDebugZipSettingName + "redact_sensitive.enabled"
)

// DebugZipSensitiveFieldsRedactionEnabled guards whether hostname / ip address and other sensitive fields
// should be redacted in the debug zip
var DebugZipSensitiveFieldsRedactionEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	DebugZipSensitiveFieldsRedactionEnabledSettingName,
	"enables or disabled hostname / ip address redaction in debug zip",
	false,
	settings.WithPublic,
	settings.WithReportable(true),
)
