import matplotlib.pyplot as plt
from IPython.display import display
from PIL import Image

def render_plot(chart_path):
    img = Image.open(chart_path)
    fig, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(img)
    ax.axis("off")
    return plt


if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('chart_path', help='string')
    args = parser.parse_args()

    chart = render_plot(chart_path = args.chart_path)