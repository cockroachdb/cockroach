package kafkaauth

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
)

var oauthOnlyParams = []string{
	SASLClientID,
	SASLClientSecret,
	SASLTokenURL,
	SASLGrantType,
	SASLScopes,
}
