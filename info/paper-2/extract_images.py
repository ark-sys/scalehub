import os
import shutil

base_path = "preliminary-plots"
new_base_path = "images"


def copy_png_files(src: str, dst: str):
    if not os.path.exists(dst):
        os.makedirs(dst)

    for root, dirs, files in os.walk(src):
        # Create corresponding directory structure in the destination
        relative_path = os.path.relpath(root, src)
        dest_dir = os.path.join(dst, relative_path)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        for file in files:
            if file.endswith(".png"):
                src_file = os.path.join(root, file)
                dest_file = os.path.join(dest_dir, file)
                shutil.copy2(src_file, dest_file)


def remove_folder(base, folder):
    # iterate over base folder and its subfolders, if export exists, remove it
    for root, dirs, files in os.walk(base):
        if folder in dirs:
            shutil.rmtree(os.path.join(root, folder))


if __name__ == "__main__":
    # copy_png_files(base_path, new_base_path)
    remove_folder(new_base_path, "export")
