

class ProduktQ_R_B(object):
    '''
    Produkt Classe, wird initialisiert mit liste von Fragen, Ratings, Beschreibung
    '''
    _registry = []
    _rating = {}

    def __init__(self, name, Q_list, R_list, B_list):

        def make_dict(self):
            merged_dict = {}
            for key in self.Q_list.keys():
                merged_dict[key] = [[self.Q_list[key]],
                                    [self.R_list[key]],
                                    [self.B_list[key]]]
            return merged_dict

        self.Q_list = Q_list
        self.R_list = R_list
        self.B_list = B_list
        self.name = name
        if name not in ProduktQ_R_B._registry:
            ProduktQ_R_B._registry.append(self)

    def compaire(self, full_awnser_list):
        '''
        Vergleicht Raitungs mit Antwort liste und retruned, GJ, MJ etc.
        '''

        ### FULL REWORK ###
        result = {}
        if set(full_awnser_list.keys()) != set(self.Q_list.keys()):
            raise ValueError("Dictionaries must have the same keys")

        for key in full_awnser_list:
            # after is different -> check if good --> if not good make different

            # BEWERTUNG
            # if gut

            # BEWERTUNG--> 0.66 & 0.33 1

            if full_awnser_list[key] == "wichtig" and self.R_list[key] == "gut":
                result[key] = "GJ"

            elif full_awnser_list[key] == "wichtig" and self.R_list[key] == "mittel":
                result[key] = "MJ"

            elif full_awnser_list[key] == "wichtig" and self.R_list[key] == "schlecht":
                result[key] = "SJ"

            elif full_awnser_list[key] == "wichtig" and self.R_list[key] == "nicht möglich":
                result[key] = "KO"

            # Optional rating

            elif full_awnser_list[key] == "Optional" and self.R_list[key] == "gut":
                result[key] = "GO"

            elif full_awnser_list[key] == "Optional" and self.R_list[key] == "mittel":
                result[key] = "MO"

            elif full_awnser_list[key] == "Optional" and self.R_list[key] == "schlecht":
                result[key] = "SO"

            elif full_awnser_list[key] == "Optional" and self.R_list[key] == "nicht möglich":
                result[key] = "NO"

            elif full_awnser_list[key] == "unwichtig":
                result[key] = "unwichtig"

        return result

    def make_rating(name, input):
        '''
        Rechtnet das Rating in % aus 
        '''
        count_A = 0
        count_B = 0
        count_C = False
        for key, values in input:
            if values == "GJ" or values == "GO":
                count_A += 100

            elif values == "MJ" or values == "MO":
                count_A += 66

            elif values == "SJ" or values == "SO":
                count_A += 33
            elif values == "KO":
                count_C = True
            elif values == "NO":
                count_A += 0

            elif values == "unwichtig":
                count_A += 0
                count_B -= 1

            count_B += 1

        if count_B == 0 or count_A == 0:
            rate = 0
            return name, rate

        elif count_C == False:
            rate = count_A / count_B
            return name, rate

        elif count_C == True:
            return name, 0
