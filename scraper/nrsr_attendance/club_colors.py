from __future__ import annotations

from typing import Final

TERM_CLUB_COLORS: Final[dict[int, dict[str, str]]] = {
    9: {
        "Klub SMER - SD": "#e43630",
        "Klub SNS": "#757575",
        "Klub KDH": "#ef8131",
        "Klub SaS": "#92be45",
        "Klub HLAS - SD": "#bf2d82",
        "Klub PS": "#039ed6",
        "Klub SLOVENSKO": "#fcd364",
    }
}


def club_colors_for_term(term_id: int) -> dict[str, str]:
    return dict(TERM_CLUB_COLORS.get(term_id, {}))
