

from subprocess import call

def extract(path, keep_original=False):
    print("Extracting {}".format(path))

    command = "pigz -d -f "
    if keep_original:
        command += "-k "
    command += "{}".format(path)

    print command

    call("pigz -d -f {}".format(path), shell=True)
    print("Extraction finished. New file path: {}".format(path[:-3]))
    return path[:-3]    #return new file path (minus '.gz')

def compress(path):
    print("Compressing {}".format(path))
    call("pigz --best -f {}".format(path), shell=True)
    print("Compression finished. New file path: {}".format("{}.gz".format(path)))
    return "{}.gz".format(path)    #rls -eturn new file path: added '.gz'