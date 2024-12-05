from client import get_consumer, recive_msg, DEFAULT_CONSUMER, recive_msg_with_logging
import sys


def main():
    args = sys.argv[1:]
    group_id = args[0] if len(args) == 1 else DEFAULT_CONSUMER

    print(f"group_id={group_id}")
    consumer = get_consumer("ALERT", group_id=group_id)
    try:
        recive_msg(consumer)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
