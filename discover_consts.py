import defeatbeta_api.utils.const as consts

print("Available constants in defeatbeta_api.utils.const:")
for name in dir(consts):
    if not name.startswith("__"):
        print(f"{name}: {getattr(consts, name)}")
