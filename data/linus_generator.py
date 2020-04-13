#! /usr/bin/env python3


TOTAL_NUM_USERS = 100
TOTAL_NUM_PROJECTS = 50
TOTAL_NUM_COMMITS = TOTAL_NUM_PROJECTS * 5

USER_FILE_NAME = "generated_users.txt"
PROJECT_FILE_NAME = "generated_projects.txt"
COMMIT_FILE_NAME = "generated_commits.txt"

def append_item(buf, item=""):
    buf.append("<")
    buf.append(item)
    buf.append(">")
    return buf


def generate_users():
    buf = []
    for i in range(TOTAL_NUM_USERS):
        append_item(buf, str(i))
        append_item(buf, f"USER_{i}")
        buf.append('\n')
    
    with open(USER_FILE_NAME, 'w') as f:
        f.write(''.join(buf))
    

def generate_projects():
    buf = []
    for i in range(TOTAL_NUM_PROJECTS):
        append_item(buf, str(i))
        append_item(buf, f"PROJECT_{i}")
        buf.append('\n')
    
    with open(PROJECT_FILE_NAME, 'w') as f:
        f.write(''.join(buf))

# project author committer
# 1 0 0
# 1 1 1
# 2 1 1
# 2 2 2
# 3 2 2
# 3 3 3
# 4 3 3

def generate_commits():
    buf = []

    for i in range(TOTAL_NUM_COMMITS):
        append_item(buf, str(i))
        append_item(buf, str(i))
        append_item(buf, str(i))
        buf.append("\n")
        append_item(buf, str(i+1))
        append_item(buf, str(i))
        append_item(buf, str(i))
        buf.append("\n")
    
    with open(COMMIT_FILE_NAME, 'w') as f:
        f.write(''.join(buf))

if __name__ == "__main__":
    generate_users()
    generate_projects()
    generate_commits()