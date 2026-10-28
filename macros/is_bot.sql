-- Bot rule, applied to commit authors, PR authors, and reviewers:
--   a GitHub user is a bot if user.type = 'Bot' OR user.login ends in '[bot]'.
-- `user_json` is the user object (e.g. payload->'user'). A missing user (a commit whose
-- email isn't linked to an account) is not a bot.
{% macro is_bot(user_json) %}
coalesce(
    ({{ user_json }}->>'type') = 'Bot' or ({{ user_json }}->>'login') like '%[bot]',
    false
)
{% endmacro %}
