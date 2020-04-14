#! /usr/bin/env python3

version_name = '7stage_4x4_'

USER_FILE_NAME = f"{version_name}generated_users.txt"
PROJECT_FILE_NAME = f"{version_name}generated_projects.txt"
COMMIT_FILE_NAME = f"{version_name}generated_commits.txt"
ANSWER_FILE_NAME = f"{version_name}generated_answer.txt"

def append_item(buf, item=""):
    buf.append("<")
    buf.append(item)
    buf.append(">")
    return buf

def write_commit(buf, project_id, user_id):
    append_item(buf, str(project_id))
    append_item(buf, str(user_id))
    append_item(buf, str(user_id))
    buf.append("\n")

def generate_users(num_users):
    with open(ANSWER_FILE_NAME, 'a') as f:
        f.write(f"Generating {num_users} users\n")
    buf = []
    for i in range(num_users):
        append_item(buf, str(i))
        append_item(buf, f"USER_{i}")
        buf.append('\n')
    
    with open(USER_FILE_NAME, 'w') as f:
        f.write(''.join(buf))
    

def generate_projects(num_projects):
    with open(ANSWER_FILE_NAME, 'a') as f:
        f.write(f"Generating {num_projects} projects\n")
    buf = []
    for i in range(num_projects):
        append_item(buf, str(i))
        append_item(buf, f"PROJECT_{i}")
        buf.append('\n')
    
    with open(PROJECT_FILE_NAME, 'w') as f:
        f.write(''.join(buf))

def generate_commits():
    buf = []
    num_stages = 7
    num_projects_per_user = 4
    num_users_per_project = 4

    project_id_count = 0
    user_id_count = 1

    users = [0]
    projects = []

    for stage in range(num_stages):
        for base_user in users:
            for project in range(num_projects_per_user):
                write_commit(buf, project_id_count, base_user)
                projects.append(project_id_count)
                project_id_count += 1
        users = []
        
        for project in projects:
            for _ in range(num_users_per_project):
                write_commit(buf, project, user_id_count)
                users.append(user_id_count)
                user_id_count += 1
        projects = []

        with open(ANSWER_FILE_NAME, 'a') as ans_f:
            ans_f.write(f'After stage {stage}:\n')
            ans_f.write(f'    Created Projects {project_id_count}\n')
            ans_f.write(f'    Created Users {user_id_count}\n')
            ans_f.write('---------------------------------------------\n')
    
    with open(COMMIT_FILE_NAME, 'w') as f:
        f.write(''.join(buf))
    
    return project_id_count, user_id_count


if __name__ == "__main__":
    with open(ANSWER_FILE_NAME, 'w') as ans_f:
        ans_f.write('') 
    num_projects, num_users = generate_commits()
    generate_users(num_users + int(num_users * 0.25))
    generate_projects(num_projects + int(num_projects * 0.25))

