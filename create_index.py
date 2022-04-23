import fs

account_name = "besciences"
# container = "test3"
container = "profiles"


def open_container(container):
    return fs.open_fs(f"azblob://{account_name}@{container}")


bfs = open_container(container)


def _gen_index(bfs, ul):
    ul.append("<ul class='nested'>")
    for f in bfs.walk.files(max_depth=1):
        ul.append(f"<li>{bfs.getinfo(f).name}</li>")
    for f in bfs.walk.dirs(max_depth=1):
        name = bfs.getinfo(f).name
        ul.append(f"<li><span class='carat'>{name}</span>")
        _gen_index(bfs.opendir(f), ul)
        ul.append("</li>")
    ul.append("</ul>")


def gen_index(bfs):
    ul = ["<ul id='myUL'>"]
    _gen_index(bfs, ul)
    ul.append("</ul>")
    return ul


ul = gen_index(bfs)

with open("index.html", "w") as f:
    f.write("<html>\n")
    f.write("<head>\n")
    f.write("<link rel='stylesheet' href='index.css'>\n")
    f.write("<script src='index.js'></script>\n")
    f.write("</head>\n")
    f.write("<body>\n")
    f.writelines(line + "\n" for line in ul)
    f.write("</body>\n")
    f.write("</html>\n")
