"""Airtable target class."""

from singer_sdk.target_base import Target
from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError

from target_airtable.sinks import AirtableSink

# Authorization method constants
AUTH_OAUTH = "oauth"
AUTH_PERSONAL_ACCESS_TOKEN = "personal_access_token"
VALID_AUTH_METHODS = [AUTH_OAUTH, AUTH_PERSONAL_ACCESS_TOKEN]

# Required fields for each auth method
OAUTH_FIELDS = ["access_token", "refresh_token", "client_id", "client_secret"]


class TargetAirtable(Target):
    """Sample target for Airtable."""

    name = "target-airtable"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "authorization_method",
            th.StringType,
            required=False,
            default="oauth",
            description="Authorization method to use: 'oauth' or 'personal_access_token' (defaults to 'oauth' for backward compatibility)"
        ),
        th.Property("access_token", th.StringType, required=False),
        th.Property("refresh_token", th.StringType, required=False),
        th.Property("client_id", th.StringType, required=False),
        th.Property("client_secret", th.StringType, required=False),
        th.Property("personal_access_token", th.StringType, required=False),
        th.Property("base_id", th.StringType, required=True),
        th.Property("records_url", th.StringType)
    ).to_dict()
    default_sink_class = AirtableSink

    def _validate_config(self, *, raise_errors: bool = True) -> tuple:
        """Validate configuration with custom auth method checks."""
        super()._validate_config(raise_errors=False)
        
        errors = []
        warnings = []
        
        # Default to OAuth for backward compatibility
        auth_method = self.config.get("authorization_method", AUTH_OAUTH)
        
        # Validate auth_method value
        if auth_method not in VALID_AUTH_METHODS:
            errors.append(
                f"authorization_method must be '{AUTH_OAUTH}' or '{AUTH_PERSONAL_ACCESS_TOKEN}', "
                f"got '{auth_method}'"
            )
        
        if auth_method == AUTH_OAUTH:
            self._validate_oauth_config(errors)
        elif auth_method == AUTH_PERSONAL_ACCESS_TOKEN:
            self._validate_pat_config(errors)
        
        if raise_errors and errors:
            raise ConfigValidationError(f"Config validation failed: {'; '.join(errors)}")
        
        return errors, warnings
    
    def _validate_oauth_config(self, errors: list) -> None:
        """Validate OAuth-specific configuration."""
        missing_fields = [field for field in OAUTH_FIELDS if not self.config.get(field)]
        if missing_fields:
            errors.append(f"OAuth authorization requires: {', '.join(missing_fields)}")
        
        if self.config.get("personal_access_token"):
            errors.append("personal_access_token should not be set when using OAuth authorization")
    
    def _validate_pat_config(self, errors: list) -> None:
        """Validate Personal Access Token configuration."""
        if not self.config.get("personal_access_token"):
            errors.append("personal_access_token is required when using personal_access_token authorization")
        
        set_oauth_fields = [field for field in OAUTH_FIELDS if self.config.get(field)]
        if set_oauth_fields:
            errors.append(
                f"OAuth fields ({', '.join(set_oauth_fields)}) should not be set "
                f"when using personal_access_token authorization"
            )

if __name__ == '__main__':
    TargetAirtable.cli()
