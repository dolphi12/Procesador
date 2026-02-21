from procesador.corrections import mostrar_menu_admin

def test_mostrar_menu_admin_prints_sections(capsys):
    mostrar_menu_admin()
    out = capsys.readouterr().out
    assert "DASHBOARD / EDITOR" in out
    assert "ADMIN" in out
    assert "AVANZADO" in out or "AVANZADO" in out.upper()
