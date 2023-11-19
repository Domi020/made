import mechanize

br = mechanize.Browser()
br.set_handle_robots(False)
br.open("https://www-genesis.destatis.de/datenbank/beta/url/c335b239")