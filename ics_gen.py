from ics import Calendar, Event

def make_calendar(json_data: dict, title: str = "Bin Collections"):
    cal = Calendar()
    for it in json_data.get("items", []):
        d = it.get("date")
        label = it.get("service", "Collection").title()
        if not d:
            continue
        ev = Event()
        ev.name = f"{label} bin collection"
        ev.begin = d
        ev.make_all_day()
        cal.events.add(ev)
    return cal

def calendar_to_str(cal: Calendar) -> str:
    return str(cal)