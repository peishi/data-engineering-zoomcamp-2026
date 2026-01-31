from pathlib import Path

current_dir = Path.cwd()
current_file = Path(__file__).name

print(f"Files in {current_dir}:")

for filepath in current_dir.iterdir():
    if filepath.name == current_file:
        continue

    print(f"  - {filepath.name}")

    if filepath.is_file():
        content = filepath.read_text(encoding='utf-8')
        print(f"    Content: {content}")

# To run this file inside docker:
# docker run -it \
#     -v $(pwd)/docker-volumes-example:/app/test \
#     --entrypoint=bash \
#     python:3.9.16-slim