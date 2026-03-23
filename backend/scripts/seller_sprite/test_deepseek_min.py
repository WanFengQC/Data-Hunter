import sys

from deepseek_classify_batch import BASE_URL, DEEPSEEK_API_KEY, client


def main():
    print("BASE_URL:", BASE_URL)
    print("API_KEY_PREFIX:", (DEEPSEEK_API_KEY[:8] + "...") if DEEPSEEK_API_KEY else "<empty>")

    try:
        resp = client.chat.completions.create(
            model="gpt-5.4",
            messages=[
                {"role": "system", "content": "You are a strict test bot."},
                {"role": "user", "content": "Reply with OK only."},
            ],
            temperature=0,
            max_tokens=8,
        )
    except Exception as exc:
        print("FAIL: request error")
        print(type(exc).__name__, str(exc))
        sys.exit(1)

    content = (resp.choices[0].message.content or "").strip()
    print("SUCCESS: response received")
    print("CONTENT:", content)


if __name__ == "__main__":
    main()
