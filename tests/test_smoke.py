def test_imports():
    import nrsr_attendance.settings as settings

    assert settings.BOT_NAME == "nrsr_attendance"
