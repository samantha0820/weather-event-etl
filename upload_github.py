import os
import base64
import requests

def upload_to_github(file_path, repo, path_in_repo, branch="main"):
    """
    Upload a local file to a GitHub repository using the GitHub API.

    Parameters:
    - file_path (str): Local path to the file to upload
    - repo (str): Repository in format "username/repo"
    - path_in_repo (str): Target path inside the repository (e.g., "output/data.csv")
    - branch (str): Branch name (default is "main")
    """

    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("Missing GitHub Token. Please set GITHUB_TOKEN environment variable.")

    api_url = f"https://api.github.com/repos/{repo}/contents/{path_in_repo}"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Local file not found: {file_path}")

    with open(file_path, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json"
    }

    # Check if the file already exists
    get_resp = requests.get(api_url, headers=headers)
    if get_resp.status_code == 200:
        sha = get_resp.json().get("sha")
        update = True
    elif get_resp.status_code == 404:
        sha = None
        update = False
    else:
        raise Exception(f"Failed to check existing file: {get_resp.status_code} {get_resp.text}")

    data = {
        "message": f"Auto-upload: {path_in_repo}",
        "content": content,
        "branch": branch
    }
    if sha:
        data["sha"] = sha

    put_resp = requests.put(api_url, headers=headers, json=data)

    if put_resp.status_code in [200, 201]:
        action = "Updated" if update else "Created"
        print(f"✅ {action} {path_in_repo} successfully on GitHub ({repo}).")
    else:
        print(f"❌ Failed to upload {path_in_repo}.")
        print(f"Status Code: {put_resp.status_code}")
        print(f"Response: {put_resp.json()}")
        raise Exception(f"GitHub upload failed for {path_in_repo}.")