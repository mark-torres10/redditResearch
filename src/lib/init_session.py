"""Initialize the parameters that are required for a given pipeline run.

Add these parameters as env vars in the Docker image, for subsequent pipeline
nodes to use.

"""
import os


class PipelineParameters:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def init_pipeline_run_parameters():
    """Generate the parameters for the pipeline run."""
    subreddits_list = ",".join(["r/politics", "r/conservative", "r/liberal"])
    number_of_threads_per_subreddit = 5
    number_of_posts_per_thread = 3
    num_responses_per_post = 2

    kwargs = {
        "subreddits_list": subreddits_list,
        "number_of_threads_per_subreddit": number_of_threads_per_subreddit,
        "number_of_posts_per_thread": number_of_posts_per_thread,
        "num_responses_per_post": num_responses_per_post,
    }

    pipeline = PipelineParameters(**kwargs)

    for key, value in vars(pipeline):
        os.environ[key] = value
