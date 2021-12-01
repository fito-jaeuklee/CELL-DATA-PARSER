from newcell.intermed_generator import IntermedGenerator


new_cell_file_path = "./resources/CLBX-5A-2233_3.0_0_1638246496_0.gp" 

if __name__ == "__main__":
    with open(new_cell_file_path, "rb") as f:
        contents = f.read()
        
    parser = IntermedGenerator.parse(contents)

    print(next(parser))
