"""
python-bigtable is made up of multiple clients, which we want to categorize separately in
the devsite table of contents.

To accomplish this, we will read in the generated toc file, remove the markdown content,
and replace it with a hand-written template
"""


import yaml
import os

STANDARD_CLIENT_SECTION = [
    "client-intro.md",
    "data-api.md",
    "table-api.md",
    "instance-api.md",
    "app-profile.md",
    "backup.md",
    "batcher.md",
    "client.md",
    "cluster.md",
    "column-family.md",
    "encryption-info.md",
    "instance.md",
    "multiprocessing.md",
    "row-data.md",
    "row-filters.md",
    "row-set.md",
    "row.md",
    "table.md",
]

SECTIONS = [
    {
        "prefix": "async_data_",
        "title": "Async Data Client",
        "items": []
    }
]

# DESIRED_ROOT = ["Overview", "Changelog", "bigtable APIs", STANDARD_CLIENT_SECTION, ASYNC_CLIENT_SECTION, "Bigtable"]

os.chdir(os.path.dirname(os.path.abspath(__file__)))

toc_file_path = os.path.join(
    os.getcwd(),  os.pardir, "_build", "html", "docfx_yaml", "toc.yml"
)
root_toc = yaml.safe_load(open(toc_file_path, "r"))[0]["items"]

# new_toc = []
# for item in root_toc:
#     if item.get("href", "").startswith(SECTIONS[0]["prefix"]):
#         SECTIONS[0]["items"].append(item)
#     else:
#         new_toc.append(item)
# new_toc.append(SECTIONS[0])

client_docs_dir = "async_data_client"
found_files = []
for file in os.listdir(os.path.join(os.getcwd(), os.pardir, client_docs_dir)):
    file_path = os.path.join(os.getcwd(), os.pardir, client_docs_dir, file)
    if file.endswith(".rst"):
        base_name = file.replace(".rst", "")
        title = open(file_path, "r").readline().replace("\n", "")
        found_files.append(
            {
                "name": title,
                "href": f"{base_name}.md",
            }
        )
new_items = root_toc + [{"name": "Async Data Client", "items": found_files}]

out_file_path = os.path.join(
    os.getcwd(),  os.pardir, "_build", "html", "docfx_yaml", "toc2.yml"
)
with open(out_file_path, "w") as f:
    f.write(yaml.dump([{"items": new_items, "name": "google-cloud-bigtable"}]))
print("done")
