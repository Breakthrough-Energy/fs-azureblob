import fs


class Index:
    def __init__(self, account_name, containers):
        self.account_name = account_name
        self.containers = containers

    def open_container(self, container):
        raise NotImplementedError

    def _link(self, container, path, name):
        base = f"https://{self.account_name}.blob.core.windows.net/{container}"
        if path != "":
            base += f"{path}"
        return f"{base}/{name}"

    def _href(self, container, path, name):
        link = self._link(container, path, name)
        return f'<a href="{link}">{name}</a>'

    def _gen_index(self, container, bfs, ul, path):
        ul.append("<ul class='nested'>")
        for f in bfs.walk.files(max_depth=1):
            href = self._href(container, path, bfs.getinfo(f).name)
            ul.append(f"<li>{href}</li>")
        for f in bfs.walk.dirs(max_depth=1):
            name = bfs.getinfo(f).name
            ul.append(f"<li><span class='caret'>{name}</span>")
            self._gen_index(container, bfs.opendir(f), ul, f"{path}{f}")
            ul.append("</li>")
        ul.append("</ul>")

    def gen_index(self, container):
        bfs = self.open_container(container)
        ul = ["<ul id='myUL'>"]
        ul.append(f"<li><span class='caret'>{container}</span>")
        self._gen_index(container, bfs, ul, "")
        ul.append("</li>")
        ul.append("</ul>")
        return ul

    def build(self, filename="index.html"):
        ul = []
        for c in self.containers:
            ul.extend(self.gen_index(c))

        with open(filename, "w") as f:
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


class IndexV1(Index):
    def open_container(self, container):
        return fs.open_fs(f"azblob://{self.account_name}@{container}")


class IndexV2(Index):
    def __init__(self, account_name, containers, credential):
        super().__init__(account_name, containers)
        self.credential = credential

    def open_container(self, container):
        return fs.open_fs(
            f"azblobv2://{self.account_name}:{self.credential}@{container}"
        )

    def _link(self, container, path, name):
        base = super()._link(container, path, name)
        return base + self.credential


if __name__ == "__main__":
    idx = IndexV1("besciences", ["profiles"])
    idx.build("index-v1.html")

    ro_token = "REPLACE_ME"
    idxv2 = IndexV2("esmi", ["profiles"], ro_token)
    idxv2.build("index-v2.html")
