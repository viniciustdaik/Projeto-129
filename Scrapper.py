from bs4 import BeautifulSoup
import pandas as pd
import requests

# URL do exoPlanetas da Nasa
START_URL = "https://en.wikipedia.org/wiki/List_of_brown_dwarfs"

# Faça uma requisição da página usando o módulo request
site = requests.get(START_URL)
print("site:", site)

soup = BeautifulSoup(site.content, "html.parser")

# Obtenha todas as tabelas da página usando o método find_all()
tbody = soup.find_all('tbody')  # _all('tbody')
# print("tbody:", tbody)

# Crie uma lista vazia
scarped_data = []

confirmed_brown_dwarfs_orbiting_primary_stars_data = []
field_brown_dwarfs_data = []

# Obtenha todas as tags <tr> da tabela
tr_tags = soup.find_all("tr")  # tbody.find_all("tr")

SDSS_reached = 0
end_reached = False
name_undefined = False
unconfirmed_brown_dwarfs_reached = False

# Loop for para extrair todas as tags <td>
for tr_tag in tr_tags:
    # print("tr_tags:", tr_tags, "\n")
    # print("tr_tag:", tr_tag)
    temp_list = []

    index = -1

    for td_tag in tr_tag.find_all("td"):
        index += 1
        data = td_tag.text.strip()

        if data == "L 34-26" and index == 0:
            end_reached = True
        elif data == "OGLE-TR-109" and index == 0:
            unconfirmed_brown_dwarfs_reached = True
        elif data == "SDSS J000013.54+255418.6 [de]" and index == 0:
            SDSS_reached = 1
            unconfirmed_brown_dwarfs_reached = False

        if data == "" and index == 0:
            name_undefined = True
        elif data != "" and index == 0 and name_undefined == True:
            name_undefined = False

        if (index == 0 or index == 5 or index == 8-SDSS_reached or index == 9-SDSS_reached) and end_reached == False and unconfirmed_brown_dwarfs_reached == False and name_undefined == False:
            # if SDSS_reached != 0:
            print("td_tag:", td_tag)
            indexToPrint = index
            if index == 8 or index == 9:
                indexToPrint = index-SDSS_reached

            if data == "":
                data = "No Info"

            print("data:", data+", index:", indexToPrint)

            # if end_reached != -4:
            #    end_reached += 1
            #    print("end_reached:", end_reached)

            # Guarde todas as linhas <td> na lista vazia que criamos anteriormente
            temp_list.append(data)

    if end_reached == False and unconfirmed_brown_dwarfs_reached == False and name_undefined == False:
        scarped_data.append(temp_list)
    if end_reached == False and unconfirmed_brown_dwarfs_reached == False and name_undefined == False and SDSS_reached == 0:
        confirmed_brown_dwarfs_orbiting_primary_stars_data.append(temp_list)
    elif end_reached == False and unconfirmed_brown_dwarfs_reached == False and name_undefined == False and SDSS_reached == 1:
        field_brown_dwarfs_data.append(temp_list)

# print("scarped_data:", scarped_data)

headers = ["name", "distance", "mass", "radius"]
# "name", "radius", "mass", "distance"

# Defina o dataframe do Pandas
star_global_df = pd.DataFrame(scarped_data, columns=headers)
star_df_1 = pd.DataFrame(
    confirmed_brown_dwarfs_orbiting_primary_stars_data, columns=headers)

# Converta para CSV

star_df_2 = pd.DataFrame(field_brown_dwarfs_data, columns=headers)

# star_df_2.to_csv('field_brown_dwarfs_data.csv', index=True, index_label="id")

list_of_brightest_stars_df = pd.read_csv("OG_list_of_brightest_stars.csv")

list_of_brightest_stars_df = list_of_brightest_stars_df.rename({
    "distance(ly)": "distance"
}, axis='columns')

# list_of_brightest_stars_df = list_of_brightest_stars_df.drop(columns=[
#                                                             "luminosity"])

print(list_of_brightest_stars_df)


def handle_data(data_df, multiply_mass_and_radius=False):
    headers = []
    for header in data_df:
        if header != "id":
            headers.append(header)

    for index in range(0, data_df.shape[0]-1):
        # print("data_df.shape:", data_df.shape[0])
        # print("index:", index)
        for header in headers:
            # print("header:", header)
            data = data_df[header][index]
            if str(type(data)) == "<class 'str'>":
                data = data.strip()
                if data.lower() == "nan" or data == "?" or data.lower() == "no info":
                    data = ""
                # if header != "radius" and header != "mass":
                    # print("data:", data)
                # print("data_df[header][index]:",
                #        data_df[header][index], "to", data)
                data_df[header][index] = data

            if (header == "radius" or header == "mass") and str(data) != "" and multiply_mass_and_radius == True:
                try:
                    data = float(data)
                    if header == "radius":
                        data = data*0.102763
                    elif header == "mass":
                        data = data*0.000954588
                    # print("data:", data)
                    # print("data_df[header][index]:",
                    #      data_df[header][index], "to", data)
                    data_df[header][index] = str(data)
                except:
                    data_df[header][index] = data_df[header][index] + \
                        " (EXCEPTION)"

    # print("data_df:", data_df)


handle_data(list_of_brightest_stars_df)
handle_data(star_df_1, True)
handle_data(star_df_2, True)
handle_data(star_global_df, True)

list_of_brightest_stars_df.to_csv("list_of_brightest_stars.csv", index=False)

star_global_df.to_csv('scraped_data.csv', index=True, index_label="id")

star_df_1.to_csv('confirmed_brown_dwarfs_orbiting_primary_stars_data.csv',
                 index=True, index_label="id")

star_df_2.to_csv('field_brown_dwarfs_data.csv', index=True, index_label="id")

print("star_df_1:", star_df_1)

merged_star_data = pd.merge(star_df_1, star_df_2)
# print("merged_star_data:", merged_star_data)

merged_list_of_stars_df = pd.merge(
    star_global_df, list_of_brightest_stars_df)

# print("merged_list_of_stars_df:", merged_list_of_stars_df)

merged_star_data.to_csv('merged_star_data.csv')

merged_list_of_stars_df.to_csv(
    'merged_list_of_stars_df.csv')

print("list_of_brightest_stars_df:", list_of_brightest_stars_df)
