from datetime import datetime

class AppLogger:
    def __init__(self):
        pass

    def log(self, file_object, log_message):
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file_object.write(f"{timestamp} - {log_message}\n")
        except Exception as e:
            print(f"Logging failed: {str(e)}")
