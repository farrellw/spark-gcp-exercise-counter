gcloud functions deploy workout-counter --entry-point com.farrellw.github.CloudFunction \
    --trigger-topic=exercise-counter-trigger \
    --runtime java11
