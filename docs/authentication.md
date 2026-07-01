# Authentication

For information about Codex CLI authentication, see [this documentation](https://developers.openai.com/codex/auth).

## Z.AI Code

Codex can store a Z.AI Code API key alongside the primary ChatGPT/OpenAI login:

```sh
codex login zai
```

For non-interactive setup:

```sh
printenv ZAI_API_KEY | codex login zai --with-api-key
```

The key is validated against Z.AI before it is saved. `codex login status`
reports `Z.AI Code: configured` when a stored key or `ZAI_API_KEY` environment
variable is available. Z.AI credentials are additive: configuring Z.AI does not
replace an existing ChatGPT login.
