
function main(splash, args)
    splash.private_mode_enanbled = true
    local start, finish = args.viewport:find('x')
    local width = tonumber(args.viewport:sub(0, start - 1))
    local height = tonumber(args.viewport:sub(finish + 1))
    splash:set_viewport_size(width, height)
    assert(splash:go{url=args.url,
        headers={["user-agent"]=args.headers["User-Agent"]}})
    assert(splash:wait(15))

    scroll_to_bottom(splash, 1, height)
    splash:runjs("window.scrollTo(0, 0);")
    assert(splash:wait(1))

    return splash:png{
        render_all=args.render_all
    }
end
