from source.pipeline_fix import run_pipeline
from source.script import timing

if __name__ == "__main__":
    timed_run_pipeline = timing(run_pipeline) 
    timed_run_pipeline() 