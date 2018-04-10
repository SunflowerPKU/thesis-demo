from git import Repo
import pandas as pd

def changed_files(c):
    if len(c.parents) != 1:
        return ""
    return ','.join([d.a_path for d in c.diff(c.parents[0])])


if __name__ == '__main__':
    repo_dir = "D:\linux"
    repo = Repo(repo_dir)
    commits = list(repo.iter_commits("master"))
    def isValid(c):
        try:
            c.authored_datetime, c.committed_datetime
            return True
        except:
            return False
    commit_list = map(lambda c: (c.hexsha, c.authored_datetime, c.author.name, c.author.email, c.committed_datetime, c.committer.name, c.committer.email, c.summary, c.message, changed_files(c)), filter(lambda c: isValid(c), commits))
    commit_df = pd.DataFrame(commit_list, columns='hexsha,authored_datetime,author_name,author_email,committed_datetime,committer_name,committer_email,summary,message,files'.split(','))
    commit_df['authored_datetime'] = pd.to_datetime(commit_df['authored_datetime'], utc=True)
    commit_df['committed_datetime'] = pd.to_datetime(commit_df['committed_datetime'], utc=True)
    commit_df.to_pickle("commits.pkl")
