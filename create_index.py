import fs

account_name = "besciences"


def open_container(container):
    return fs.open_fs(f"azblob://{account_name}@{container}")


def _url(container, path, name):
    base = f"https://besciences.blob.core.windows.net/{container}"
    if path != "":
        base += f"{path}"
    link = f"{base}/{name}"
    return f'<a href="{link}">{name}</a>'


def _gen_index(container, bfs, ul, path):
    ul.append("<ul class='nested'>")
    for f in bfs.walk.files(max_depth=1):
        href = _url(container, path, bfs.getinfo(f).name)
        ul.append(f"<li>{href}</li>")
    for f in bfs.walk.dirs(max_depth=1):
        name = bfs.getinfo(f).name
        ul.append(f"<li><span class='caret'>{name}</span>")
        _gen_index(container, bfs.opendir(f), ul, f"{path}{f}")
        ul.append("</li>")
    ul.append("</ul>")


def gen_index(container):
    bfs = open_container(container)
    ul = ["<ul id='myUL'>"]
    ul.append(f"<li><span class='caret'>{container}</span>")
    _gen_index(container, bfs, ul, "")
    ul.append("</li>")
    ul.append("</ul>")
    return ul


ul = []
ul.extend(gen_index("profiles"))
ul.extend(gen_index("scenariodata"))

with open("index.html", "w") as f:
    f.write("<!DOCTYPE html>\n")
    f.write("<html>\n")
    f.write("<head>\n")
    f.write('<link rel="stylesheet" href="index.css">\n')
    f.write("</head>\n")
    f.write("<body>\n")
    f.writelines(line + "\n" for line in ul)
    f.write("</body>\n")
    f.write('<script src="index.js"></script>\n')
    f.write("</html>\n")
