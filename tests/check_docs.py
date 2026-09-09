"""Check local Markdown links, translation structure and release references."""

from pathlib import Path
import re
import sys
from urllib.parse import unquote


root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parents[1]
excluded = {".git", "build", "dist", ".pytest_cache", "__pycache__"}
documents = [p for p in root.rglob("*.md") if not excluded.intersection(p.relative_to(root).parts)]
errors = []
links = 0
pairs = 0


def headings(text):
    result = []
    fenced = False
    for line in text.splitlines():
        if line.startswith("```"):
            fenced = not fenced
        if not fenced and (match := re.match(r"^(#+) (.*)", line)):
            result.append(match.groups())
    return result


def anchors(text):
    result = set()
    duplicates = {}
    for _, title in headings(text):
        title = re.sub(r"\[([^]]+)\]\([^)]*\)", r"\1", title)
        slug = re.sub(r"[^\w\- ]", "", title.lower()).replace(" ", "-")
        count = duplicates.get(slug, 0)
        duplicates[slug] = count + 1
        result.add(f"{slug}-{count}" if count else slug)
    return result


for path in documents:
    content = path.read_text()
    if re.search(r"\b0\.[67]\.0\b", content):
        errors.append(f"{path}: stale release reference")
    if path.name.endswith("_cn.md"):
        english = path.with_name(path.name.replace("_cn.md", ".md"))
        if not english.exists():
            errors.append(f"{path}: missing English translation")
        else:
            pairs += 1
            chinese_structure = [len(level) for level, _ in headings(content)]
            english_structure = [len(level) for level, _ in headings(english.read_text())]
            if chinese_structure != english_structure:
                errors.append(f"{path}: translation heading structure differs")
    for target in re.findall(r"\[[^\]]*\]\(([^)]+)\)", content):
        if re.match(r"[a-zA-Z][\w+.-]*:", target):
            continue
        target = unquote(target.strip("<>"))
        file, _, anchor = target.partition("#")
        resolved = (path.parent / file).resolve() if file else path
        links += 1
        if not resolved.exists():
            errors.append(f"{path}: missing link target {target}")
        elif anchor and resolved.suffix == ".md" and anchor not in anchors(resolved.read_text()):
            errors.append(f"{path}: missing heading {target}")

for error in errors:
    print(error, file=sys.stderr)
print(f"Checked {len(documents)} documents, {pairs} translation pairs, {links} local links")
sys.exit(bool(errors))
