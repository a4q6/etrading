if __name__ == '__main__':
    from glob import glob
    import shutil
    from pathlib import Path
    files = glob("data/tp/TP-*-*.log")
    
    for file in files:
        print(file)
        path = Path(file)
        sym = path.name.split("-")[-1].replace(".log", "")
        sym = sym.replace("_", "").upper()
        prefix = "-".join(path.name.split("-")[:2])
        new_name = f"{prefix}-{sym}.log"
        new_path = path.parent.joinpath(new_name)
        shutil.move(path, new_path)
        print(f"mv {path} -> {new_path}")
