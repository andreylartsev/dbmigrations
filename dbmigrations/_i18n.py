from __future__ import annotations

import gettext
from pathlib import Path

from _constants import TRANSLATIONS_DOMAIN, TRANSLATIONS_SUBDIRECTORY

# Active translator. Rebinding this module-level variable via setup_translations()
# switches the UI language. `_` is a stable callable that reads the *current*
# translator on every invocation, so modules that did `from _i18n import _`
# before setup_translations() still see the updated language at call time.

_translator = gettext.gettext


def _(text: str) -> str:
    return _translator(text)


def setup_translations(lang: str | None) -> None:
    global _translator
    translations_dir = Path(__file__).resolve().parent.joinpath(TRANSLATIONS_SUBDIRECTORY)
    translator = gettext.translation(
        TRANSLATIONS_DOMAIN,
        localedir=str(translations_dir) if translations_dir.exists() else None,
        # in case of None it takes selected system languages from the system i.e. LC_MESSAGES variable
        languages=[lang] if lang else None,
        fallback=True,  # return NoneTranslator if nothing found
    )
    _translator = translator.gettext