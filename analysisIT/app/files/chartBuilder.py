import processing
import matplotlib.pyplot as plt
import numpy as np


def show_chart_1():
    data_frame = processing.read_data()
    if data_frame is not None:
        data_frame_pie = data_frame.groupby('experience', as_index=False).id.count()
        x = data_frame_pie.experience
        y = data_frame_pie.id
        fig = plt.figure(figsize=(10, 5))
        ax = fig.add_subplot()
        ax.pie(y, labels=x, autopct='%1.1f%%', textprops={'fontsize': 11})
        return plt


def show_chart_2():
    data_frame = processing.read_data()

    schedule_data = processing.salary_filter(data_frame)
    schedule_data = schedule_data.pivot_table(index='id', columns='schedule', values='salary_mean')
    schedule_list = ['Полный день', 'Удаленная работа', 'Гибкий график', 'Сменный график']
    data_list, list_name = processing.filter_valid_data_from_dataframe(schedule_data, schedule_list)

    fig = plt.figure(figsize=(15, 10))
    ax = fig.add_subplot()
    bp = ax.boxplot(data_list, patch_artist=True)
    colors = ['ForestGreen', 'IndianRed', 'goldenrod', 'CadetBlue']

    valid_data_list = [data for data in data_list if len(data) > 1]
    medians = [np.median(data) for data in valid_data_list]

    for box, color, median, i in zip(bp['boxes'], colors, medians, range(1, len(valid_data_list) + 1)):
        box.set_facecolor(color)
        plt.setp(bp['medians'], linewidth=2)
        plt.text(i, median, f'{median}', va='bottom', ha="center", bbox=dict(facecolor="w", alpha=0.2))

    ax.ticklabel_format(style='plain', axis='y')
    ax.set_ylim(0, 580000)
    ax.set_title("Медианные предлагаемые зарплаты в ИТ-сфере в зависимости от графика работы")
    ax.set_yticks(range(0, 580000, 50000))
    ax.set_xticks(range(1, len(list_name) + 1))
    ax.set_xticklabels(list_name)
    ax.set_ylabel("Зарплата, руб.")
    ax.grid()


def show_chart_3():
    data_frame = processing.read_data()
    data_area = processing.salary_filter(data_frame)

    are_list = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', 'Владивосток', 'Казань',
                'Нижний Новгород', 'Ростов-на-Дону', 'Челябинск', 'Воронеж']
    data_area = data_area[data_area.area_name.isin(are_list) == True]
    data_area = data_area.pivot_table(index='id', columns='area_name', values='salary_mean')
    data_list, list_name = processing.filter_valid_data_from_dataframe(data_area, are_list)

    fig = plt.figure(figsize=(15, 10))
    ax = fig.add_subplot()
    bp = ax.boxplot(data_list, patch_artist=True)
    colors = ['ForestGreen', 'IndianRed', 'goldenrod', 'CadetBlue', 'RoyalBlue', 'MediumTurquoise', 'SeaGreen',
              'SkyBlue', 'DarkKhaki', 'Burlywood']

    valid_data_list = [data for data in data_list if len(data) > 1]
    medians = [np.median(data) for data in valid_data_list]

    for box, color, median, i in zip(bp['boxes'], colors, medians, range(1, len(valid_data_list) + 1)):
        box.set_facecolor(color)
        plt.setp(bp['medians'], linewidth=2)
        plt.text(i, median, f'{median}', va='bottom', ha="center", bbox=dict(facecolor="w", alpha=0.2))

    ax.set_ylim(0, 400000)
    ax.ticklabel_format(style='plain', axis='y')
    ax.set_xticks(range(1, len(list_name) + 1))
    ax.set_xticklabels(list_name, rotation=20)

    ax.set_yticks(range(0, 400000, 50000))
    ax.set_title("Медианные предлагаемые зарплаты по городам")
    ax.set_ylabel("Зарплата")
    ax.grid()


def show_chart_4():
    data_frame = processing.read_data()

    data_s = processing.salary_filter(data_frame)
    data_s = data_s.pivot_table(index='id', columns='experience', values='salary_mean')

    roles_list = ['Нет опыта', 'От 1 года до 3 лет', 'От 3 до 6 лет', 'Более 6 лет']
    data_list, list_name = processing.filter_valid_data_from_dataframe(data_s, roles_list)

    fig = plt.figure(figsize=(15, 10))
    ax = fig.add_subplot()
    bp = ax.boxplot(data_list, patch_artist=True)
    colors = ['ForestGreen', 'IndianRed', 'goldenrod', 'CadetBlue']

    valid_data_list = [data for data in data_list if len(data) > 1]
    medians = [np.median(data) for data in valid_data_list]

    for box, color, median, i in zip(bp['boxes'], colors, medians, range(1, len(valid_data_list) + 1)):
        box.set_facecolor(color)
        plt.setp(bp['medians'], linewidth=2)
        plt.text(i, median, f'{median}', va='bottom', ha="center", bbox=dict(facecolor="w", alpha=0.2))

    ax.ticklabel_format(style='plain', axis='y')
    ax.set_ylim(0, 580000)
    ax.set_title("Медианные предлагаемые зарплаты в ИТ-сфере в зависимости от опыта")
    ax.set_yticks(range(0, 580000, 50000))
    ax.set_xticks(range(1, len(list_name) + 1))
    ax.set_xticklabels(list_name)
    ax.set_ylabel("Зарплата, руб.")
    ax.grid()


def show_chart_5():
    data_frame = processing.read_data()
    data_k = processing.salary_filter(data_frame)

    data_k = data_k.pivot_table(index='id', columns='employment', values='salary_mean')
    roles_list = ['Полная занятость', 'Частичная занятость', 'Стажировка', 'Проектная работа']
    data_list, list_name = processing.filter_valid_data_from_dataframe(data_k, roles_list)

    fig = plt.figure(figsize=(15, 10))
    ax = fig.add_subplot()
    bp = ax.boxplot(data_list, patch_artist=True)
    colors = ['ForestGreen', 'IndianRed', 'goldenrod', 'CadetBlue']

    valid_data_list = [data for data in data_list if len(data) > 1]
    medians = [np.median(data) for data in valid_data_list]

    for box, color, median, i in zip(bp['boxes'], colors, medians, range(1, len(valid_data_list) + 1)):
        box.set_facecolor(color)
        plt.setp(bp['medians'], linewidth=2)
        plt.text(i, median, f'{median}', va='bottom', ha="center", bbox=dict(facecolor="w", alpha=0.2))

    ax.ticklabel_format(style='plain', axis='y')
    ax.set_ylim(0, 580000)
    ax.set_title("Медианные предлагаемые зарплаты в ИТ-сфере в зависимости от опыта")
    ax.set_yticks(range(0, 580000, 50000))
    ax.set_xticks(range(1, len(list_name) + 1))
    ax.set_xticklabels(list_name)
    ax.set_ylabel("Зарплата, руб.")
    ax.grid()


def show_chart_6():
    data_frame = processing.read_data()

    data_ex = data_frame[(data_frame.experience.isna() == False) & (data_frame.schedule.isna() == False)]
    data_ex = data_ex[data_ex.professional_roles_name == 'Программист, разработчик']

    data_ex = data_ex.pivot_table(index='experience', columns='schedule', values='id', aggfunc='count', fill_value=0)
    x = data_ex.index
    y1 = data_ex['Полный день']
    y2 = data_ex['Удаленная работа']

    fig = plt.figure(figsize=(15, 10))
    ax1 = fig.add_subplot(1, 2, 1)
    ax2 = fig.add_subplot(1, 2, 2)
    ax1.pie(y1, labels=x, autopct='%1.1f%%', textprops={'fontsize': 11})
    ax2.pie(y2, labels=x, autopct='%1.1f%%', textprops={'fontsize': 11})
    ax1.set_xlabel("Полный день", size=15)
    ax2.set_xlabel("Удаленная работа", size=15)
