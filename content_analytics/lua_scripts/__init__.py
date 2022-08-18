# Lua scripts for splash when rendering requires complex logic (like closing popups)

import pkg_resources


# Helper function to scroll to bottom
with pkg_resources.resource_stream('content_analytics', 'lua_scripts/scroll_to_bottom.lua') as handle:
    SCROLL_TO_BOTTOM = handle.readlines()

# Close popup, scroll to bottom, then back to the top, then make screenshot
with pkg_resources.resource_stream('content_analytics', 'lua_scripts/default_main.lua') as handle:
    DEFAULT_MAIN = ''.join(SCROLL_TO_BOTTOM + handle.readlines())
