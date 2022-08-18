
function scroll_to_bottom(splash, delay, height)
    local get_scroll_top = splash:jsfunc(
        "function() {return document.body.scrollTop;}"
    )
    local scroll_to = splash:jsfunc("window.scrollTo")
    while true do
        local old_top = get_scroll_top()
        if old_top == nil then
            break
        end
        scroll_to(0, old_top + height - 100)
        assert(splash:wait(delay))
        local new_top = get_scroll_top()
        if old_top == new_top then
            break
        end
    end
end
