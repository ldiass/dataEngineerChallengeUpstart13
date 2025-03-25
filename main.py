##Run whole pipeline
def main():
    with open("src/data_extraction.py") as f:
        exec(f.read(), globals())
    with open("src/publish_product.py") as f:
        exec(f.read(), globals())
    with open("src/publish_orders.py") as f:
        exec(f.read(), globals())
    with open("src/final_questions.py") as f:
        exec(f.read(), globals())

if __name__=='__main__':
    main()