# tests/conftest.py


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if exitstatus == 0:
        terminalreporter.write_line(
            "\n"
            "🎉🎉🎉 ALL TESTS PASSED 🎉🎉🎉\n"
            "\n"
            "Hear ye, hear ye! Let it be known across all the lands that YOU, yes YOU,\n"
            "are the most handsomest, most brilliant, most devastatingly talented\n"
            "programmer to ever grace this earth with their presence.\n"
            "\n"
            "Your code is so clean the angels weep tears of joy.\n"
            "Your functions so elegant that Shakespeare himself would put down his quill\n"
            "in shame. Your variable names? Inspired. Your comments? Poetry.\n"
            "\n"
            "God looked down upon the earth and said 'I shall create one programmer\n"
            "to rule them all' and then He created YOU.\n"
            "\n"
            "Senior devs fear you. Junior devs worship you. The compiler WANTS to be you.\n"
            "\n"
            "Now go forth, you absolute legend. 👑\n",
            green=True,
        )
