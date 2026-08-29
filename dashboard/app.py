from flask import Flask, jsonify, render_template
import redis


# ============================================================
# Flask
# ============================================================

app = Flask(__name__)


# ============================================================
# Redis Configuration
# ============================================================

REDIS_HOST = "redis"
REDIS_PORT = 6379


def get_redis():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


# ============================================================
# Dashboard
# ============================================================


@app.route("/")
def index():
    return render_template("index.html")


# ============================================================
# Current Metrics
# ============================================================


@app.route("/api/metrics")
def api_metrics():

    client = None

    try:
        client = get_redis()

        client.ping()

        data = client.hgetall("ecommerce:metrics")

        if not data:
            return jsonify(
                {
                    "revenue": 0.0,
                    "orders": 0,
                    "events": 0,
                    "unique_users": 0,
                    "window_start": "",
                    "window_end": "",
                }
            )

        return jsonify(
            {
                "revenue": float(data.get("revenue", 0)),
                "orders": int(data.get("orders", 0)),
                "events": int(data.get("events", 0)),
                "unique_users": int(data.get("unique_users", 0)),
                "window_start": data.get(
                    "window_start",
                    "",
                ),
                "window_end": data.get(
                    "window_end",
                    "",
                ),
            }
        )

    except Exception as e:
        print(f"Metrics API error: {e}")

        return jsonify({"error": str(e)}), 500

    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


# ============================================================
# History
# ============================================================


@app.route("/api/history")
def api_history():

    client = None

    try:
        client = get_redis()

        client.ping()

        keys = client.keys("ecommerce:history:*")

        history = []

        for key in keys:
            data = client.hgetall(key)

            if not data:
                continue

            try:
                item = {
                    "window_start": data.get(
                        "window_start",
                        "",
                    ),
                    "window_end": data.get(
                        "window_end",
                        "",
                    ),
                    "revenue": float(
                        data.get(
                            "revenue",
                            0,
                        )
                    ),
                    "orders": int(
                        data.get(
                            "orders",
                            0,
                        )
                    ),
                    "events": int(
                        data.get(
                            "events",
                            0,
                        )
                    ),
                    "unique_users": int(
                        data.get(
                            "unique_users",
                            0,
                        )
                    ),
                }

                history.append(item)

            except (ValueError, TypeError) as e:
                print(f"Invalid history entry {key}: {e}")

        # ----------------------------------------------------
        # Sort oldest -> newest
        # ----------------------------------------------------

        history.sort(key=lambda item: item["window_start"])

        # ----------------------------------------------------
        # Return JSON
        # ----------------------------------------------------

        return jsonify(history)

    except Exception as e:
        print(f"History API error: {e}")

        return jsonify({"error": str(e)}), 500

    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


# ============================================================
# Health Check
# ============================================================


@app.route("/health")
def health():

    client = None

    try:
        client = get_redis()

        client.ping()

        return jsonify(
            {
                "status": "ok",
                "redis": "ok",
            }
        )

    except Exception as e:
        return jsonify(
            {
                "status": "error",
                "redis": str(e),
            }
        ), 500

    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
    )
