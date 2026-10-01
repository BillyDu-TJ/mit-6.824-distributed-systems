import os
import subprocess
import requests
import pymupdf
from bs4 import BeautifulSoup

BASE_URL = "https://pdos.csail.mit.edu/6.824"
DOCS_DIR = os.path.abspath("docs")
PAPERS_DIR = os.path.join(DOCS_DIR, "papers")
NOTES_DIR = os.path.join(DOCS_DIR, "notes")
LABS_DIR = os.path.join(DOCS_DIR, "labs")

os.makedirs(PAPERS_DIR, exist_ok=True)
os.makedirs(NOTES_DIR, exist_ok=True)
os.makedirs(LABS_DIR, exist_ok=True)

def curl_download(url, dest_path):
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000:
        print(f"[SKIP] {dest_path} already exists ({os.path.getsize(dest_path)} bytes)")
        return
    print(f"[DOWNLOADING] {url} -> {dest_path}")
    ret = subprocess.run(["curl", "-sL", "-o", dest_path, url], capture_output=True)
    if ret.returncode != 0:
        print(f"[ERROR] Failed to download {url}")
    else:
        print(f"[OK] Downloaded {dest_path} ({os.path.getsize(dest_path)} bytes)")

# 1. Download Papers
papers = [
    "mapreduce.pdf",
    "gfs.pdf",
    "raft-extended.pdf",
    "paxos-simple.pdf",
    "zookeeper.pdf",
    "spanner.pdf",
    "cr-osdi04.pdf",
    "farm-2015.pdf",
    "memcache-fb.pdf",
    "bitcoin.pdf",
    "castro-practicalbft.pdf"
]

print("=== Downloading Papers ===")
for p in papers:
    curl_download(f"{BASE_URL}/papers/{p}", os.path.join(PAPERS_DIR, p))

# 2. Download Lecture Notes
notes = [
    "l01.txt",
    "l-rpc.txt",
    "l-gfs.txt",
    "l-paxos.txt",
    "Go-MIT6824-2026.pdf",
    "l-raft.txt",
    "l-raft2.txt",
    "l-linearizability.txt",
    "l-zookeeper.txt",
    "l-raft-QA.txt",
    "l-2pc.txt",
    "l-spanner.txt",
    "l-cr.txt",
    "l-farm.txt",
    "l-memcached.txt",
    "l-ray.txt",
    "l-sundr.txt",
    "l-bitcoin.txt",
    "65840-pbft.pdf"
]

print("\n=== Downloading Lecture Notes ===")
for n in notes:
    curl_download(f"{BASE_URL}/notes/{n}", os.path.join(NOTES_DIR, n))

# 3. Lab HTML to PDF Conversion
CSS_STYLE = """
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    font-size: 10.5pt;
    line-height: 1.5;
    color: #24292e;
}
h1 {
    font-size: 18pt;
    color: #0366d6;
    border-bottom: 2px solid #eaecef;
    padding-bottom: 6px;
    margin-top: 16px;
    margin-bottom: 12px;
}
h2 {
    font-size: 14pt;
    color: #24292e;
    margin-top: 14px;
    margin-bottom: 10px;
}
h3 {
    font-size: 12pt;
    color: #0366d6;
    border-bottom: 1px solid #eaecef;
    padding-bottom: 4px;
    margin-top: 14px;
    margin-bottom: 8px;
}
h4 {
    font-size: 11pt;
    color: #24292e;
    margin-top: 10px;
    margin-bottom: 6px;
}
p {
    margin-top: 0;
    margin-bottom: 10px;
}
ul, ol {
    margin-top: 0;
    margin-bottom: 10px;
    padding-left: 24px;
}
li {
    margin-bottom: 4px;
}
code {
    font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, Courier, monospace;
    font-size: 9.5pt;
    background-color: #f6f8fa;
    padding: 2px 4px;
    border-radius: 3px;
    border: 1px solid #e1e4e8;
}
pre {
    font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, Courier, monospace;
    font-size: 9pt;
    background-color: #f6f8fa;
    border: 1px solid #e1e4e8;
    border-radius: 4px;
    padding: 8px 12px;
    line-height: 1.4;
    white-space: pre-wrap;
    word-break: break-all;
    margin-bottom: 12px;
}
pre code {
    background: transparent;
    padding: 0;
    border: none;
}
.note, .tip, .important {
    background-color: #f1f8ff;
    border-left: 4px solid #0366d6;
    padding: 8px 12px;
    margin-bottom: 10px;
}
table {
    border-collapse: collapse;
    width: 100%;
    margin-bottom: 12px;
}
th, td {
    border: 1px solid #dfe2e5;
    padding: 6px 12px;
    text-align: left;
}
th {
    background-color: #f6f8fa;
}
"""

def convert_html_to_pdf(html_content, output_pdf_path, title="MIT 6.5840"):
    soup = BeautifulSoup(html_content, "html.parser")

    # Clean up relative stylesheet references and head
    for s in soup.find_all("link", rel="stylesheet"):
        s.decompose()
    for script in soup.find_all("script"):
        script.decompose()

    # Fix relative image paths if any
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if src == "shardkv.png" or src.endswith("shardkv.png"):
            img["src"] = os.path.join(LABS_DIR, "shardkv.png")

    cleaned_html = str(soup)

    story = pymupdf.Story(html=cleaned_html, user_css=CSS_STYLE)
    writer = pymupdf.DocumentWriter(output_pdf_path)
    mediabox = pymupdf.Rect(0, 0, 595, 842) # A4
    fillbox = pymupdf.Rect(40, 40, 595 - 40, 842 - 40)
    
    more = 1
    page_count = 0
    while more:
        device = writer.begin_page(mediabox)
        more, _ = story.place(fillbox)
        story.draw(device)
        writer.end_page()
        page_count += 1
    writer.close()
    print(f"[PDF CREATED] {output_pdf_path} ({page_count} pages, {os.path.getsize(output_pdf_path)} bytes)")

labs_to_fetch = [
    ("Lab1-MapReduce", f"{BASE_URL}/labs/lab-mr.html"),
    ("Lab2-KeyValue-Server", f"{BASE_URL}/labs/lab-kvsrv1.html"),
    ("Lab3-Raft", f"{BASE_URL}/labs/lab-raft1.html"),
    ("Lab4-KVRaft", f"{BASE_URL}/labs/lab-kvraft1.html"),
    ("Lab5-ShardedKV", f"{BASE_URL}/labs/lab-shard1.html"),
    ("Guidance-and-Collab", [f"{BASE_URL}/labs/guidance.html", f"{BASE_URL}/labs/collab.html"])
]

print("\n=== Fetching Lab HTMLs and Building PDFs ===")
for name, target in labs_to_fetch:
    if isinstance(target, list):
        combined_html = ""
        for u in target:
            res = subprocess.run(["curl", "-sL", u], capture_output=True, text=True)
            combined_html += res.stdout + "<hr/><hr/>"
        html_content = combined_html
        # Also save raw HTML
        with open(os.path.join(LABS_DIR, f"{name}.html"), "w") as f:
            f.write(html_content)
    else:
        res = subprocess.run(["curl", "-sL", target], capture_output=True, text=True)
        html_content = res.stdout
        # Also save raw HTML
        with open(os.path.join(LABS_DIR, f"{name}.html"), "w") as f:
            f.write(html_content)

    pdf_path = os.path.join(LABS_DIR, f"{name}.pdf")
    convert_html_to_pdf(html_content, pdf_path, title=name)

print("\nAll docs successfully generated!")
