SELECT '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/Paris")).strftime("%d/%m %H:%M:%S") }}' as run_started_Paris