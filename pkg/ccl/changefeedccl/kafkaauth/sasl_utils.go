package kafkaauth

// TODO: is there any reason to have these as constants? if not, remove
const (
	SASLUser      = "sasl_user"
	SASLPassword  = "sasl_password"
	SASLHandshake = "sasl_handshake"
	SASLMechanism = "sasl_mechanism"
	SASLEnabled   = "sasl_enabled"

	SASLClientID     = "sasl_client_id"
	SASLClientSecret = "sasl_client_secret"
	SASLTokenURL     = "sasl_token_url"
	SASLGrantType    = "sasl_grant_type"
	SASLScopes       = "sasl_scopes"

	SASLAWSIAMRoleArn     = `sasl_aws_iam_role_arn`
	SASLAWSRegion         = `sasl_aws_region`
	SASLAWSIAMSessionName = `sasl_aws_iam_session_name`
)

var oauthOnlyParams = []string{
	SASLClientID,
	SASLClientSecret,
	SASLTokenURL,
	SASLGrantType,
	SASLScopes,
}
