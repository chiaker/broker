from dataclasses import dataclass


@dataclass(slots=True)
class StompFrame:
    command: str
    headers: dict[str, str]
    body: str


def parse_stomp_frame(raw_frame: str) -> StompFrame:
    content = raw_frame.rstrip("\x00")
    content = content.replace("\r\n", "\n")
    head, _, body = content.partition("\n\n")
    lines = [line for line in head.split("\n") if line]
    if not lines:
        raise ValueError("Empty STOMP frame")
    command = lines[0].strip()
    headers: dict[str, str] = {}
    for line in lines[1:]:
        key, sep, value = line.partition(":")
        if not sep:
            continue
        headers[key.strip()] = value.strip()
    return StompFrame(command=command, headers=headers, body=body)


def build_stomp_frame(command: str, headers: dict[str, str] | None = None, body: str = "") -> str:
    lines = [command]
    for key, value in (headers or {}).items():
        lines.append(f"{key}:{value}")
    lines.append("")
    lines.append(body)
    return "\n".join(lines) + "\x00"
