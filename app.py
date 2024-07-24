from flask import Flask, render_template, request


app = Flask(__name__)


@app.route("/")
@app.route("/index")
def main():
    user = {"username": "willcoomans", "age": 20, "role": "Student"}
    transcript = [
        {
            "course_name": "DATA100",
            "grade": "A+"
        },
        {
            "course_name": "CS70",
            "grade": "A"
        },
        {
            "course_name": "CS61B",
            "grade": "A+"
        }
    ]
    return render_template("index.html", user=user, transcript=transcript)
