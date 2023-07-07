import datetime
import openai
import random

class EventGenerator():
    def __init__(self):

        self.current_day_str = None
        self.list_of_contexts = []
        self.deployment_name = None

        self.initialize()

        # Create an empty list to store the descriptions of the events for today:
        self.descriptions_events_today = []

    def initialize(self,):
        self.save_current_day()
        self.save_gpt_settings()
        self.save_prompts()


    def save_current_day(self,):
        current_day = datetime.date.today()

        month = current_day.month
        day = current_day.day

        month_list = ["Gener", "Febrer", "Març", "Abril", "Maig", "Juny", "Juliol", "Agost", "Setembre", "Octubre", "Novembre", "Desembre"]

        self.current_day_str = str(day) + " de " + month_list[month-1]


    def save_gpt_settings(self,):
        #AZURE OPENAI
        #Conect to Azure OpenAI model:
        openai.api_key = "a87266acc05c46519ec00fb8bc86fbc5"
        openai.api_base = "https://openaiabel12.openai.azure.com/" # your endpoint
        openai.api_type = 'azure'
        openai.api_version = '2023-05-15' # this may change in the future
        self.deployment_name='gpt35turbo' #This will correspond to the custom name you chose for your deployment when you deployed a model.


    def save_prompts(self,):
        events_casal = ["un concert", "una actuació", "una obra de teatre", "una xerrada", "una festa de música electronica", "un sopar a l'aire lliure de festa major", "una festa popular"]

        event_casal = random.choice(events_casal)

        context_generation_night_events =  [
            {

                "role": "user",

                "content": f"""

                    Crea un event nocturn fictici com {event_casal} que es celebri a la plaça del casal de Canyelles

                    el dia {self.current_day_str}. L'event serà publicat a les xarxes socials. Es important que la descripció del event sigui molt extensa i sigui el més creible possible, que inclogui la data i la hora

                    a la que comença i que també inclogui la localització del event, que en aquest cas és la Plaça del casal. Respon únicament amb la descripcó de l'event.

                """

            },
        ]



        events_pavello = ["un torneig de bàsquet", "un torneig de voleibol", "un torneig de futbol sala amb partits amistosos entre equips locals de veïns", "un partit de l'equip femení de hoquei sobre patins de la lliga catalana contra un altre equip regional"

                            "una exibició o competició de patinatge artísitc", "un torneig de tennis taula", "la celebració escolar de l'institut de Canyelles", "les jornades de la ciéncia per els més petits", "un partit de fútbol de l'equip masculí de futbol sala de la lliga catalana"]

        event_pavello = random.choice(events_pavello)




        context_generation_sports_events_pavello =  [

            {

                "role": "user",

                "content": f"""

                    Crea un event esportiu fictici com {event_pavello} que es pugui celebrar al pavelló municipal del municipi de Canyelles

                    el dia {self.current_day_str}. L'event serà publicat a les xarxes socials. Es important que la descripció del event sigui molt extensa i que inclogui la data i la hora

                    a la que comença i la localització del event, que en aquest cas és el pavelló municipal. Respon únicament amb la descripcó de l'event.

                """

            },

        ]

        context_generation_sports_events_futbol_field1 =  [

            {

                "role": "user",

                "content": f"""

                    Crea un event esportiu fictici que es pugui celebrar al camp de futbol municipal del municipi de Canyelles

                    el dia {self.current_day_str}. Es important que la descripció del event sigui molt extensa i que inclogui la data i la hora

                    a la que comença i la localització del event, que en aquest cas és el camp de futbol municipal. Respon únicament amb la descripcó de l'event i no diguis que és fictici.

                """

            },

        ]

        events_futbol_field2 = ["un torneig de bàsquet", "un torneig de voleibol adaptant el camp de fútbol amb xarxes", "un torneig de tennis taula a l'aire lliure", "un torneig de futbol amistós amb equips de l'institut de Canyelles", "un partit de fútbol de l'equip masculí de futbol sala de la lliga catalana"

                                "unes carreres de cotxes teledirigits", "unes jornades de les ciéncies naturals"]

        event_futbol_field2 = random.choice(events_futbol_field2)

        context_generation_sports_events_futbol_field2 =  [

            {

                "role": "user",

                "content": f"""

                    Creea la descripció d'un event que es pugui celebrar a un camp de fútbol relacionat amb {event_futbol_field2} el dia {self.current_day_str}.

                    Es important que la descripció del event sigui molt extensa i que inclogui la data i la hora

                    a la que comença i la localització del event, que en aquest cas és el camp de fútbol de la urbanització Vora Sitges.

                """

            },

        ]

        context_generation_events_school =  [

            {

                "role": "user",

                "content": f"""

                    Crea un event que es pugui celebrar a la Escola Pública Sant Nicolau de Canyelles

                    el dia {self.current_day_str}. Es important que la descripció del event sigui molt extensa i que inclogui la data i la hora

                    a la que comença i la localització del event, que en aquest cas és la Escola Pública Sant Nicolau de Canyelles. Respon únicament amb la descripcó de l'event.

                """

            },

        ]

        events_hotelito = ["una reunió de l'asociacció de veïns", "una conferencia sobre botànica", "una sessió de cinema a la fresca", "un concert de música en directe"

                                "Una sessió de ioga als jardins", "un sopar pouplar", "un sopar popular estil barbacoa", "una jornada de lectura per els més petits"]

        event_hotelito = random.choice(events_hotelito)

        context_generation_events_hotelito =  [

            {

                "role": "user",

                "content": f"""

                    Crea un event que es pugui celebrar als jardins de l'hotelito que està situat a la urbanització California

                    el dia {self.current_day_str}. l'event ha d'estar relacionat amb {event_hotelito}.

                    Es important que la descripció del event sigui molt extensa i que inclogui la data i la hora

                    a la que comença i la localització del event, que en aquest cas és els jardins el Hotelito.

                """

            },

        ]

        events_hotelito_night = ["una sessió de DJ", "un concert acústic d'un artista local", "un concert de rock", "concerts de grups musicals formats per joves del municipi"

                                    "una obra de teatre organitzada per l'associació de teatre de Canyelles"]

        event_hotelito_night = random.choice(events_hotelito_night)

        context_generation_events_hotelito_night = [

            {

                "role": "user",

                "content": f"""

                    Crea un event que es puguin celebrar als jardins de l'hotelito que estan situats a la urbanització California

                    el dia {self.current_day_str}. L'event ha d'anar relacionat amb l'oci nocturn com {event_hotelito_night}.

                    Es important que la descripció del event sigui molt extensa (com a mínim 100 paraules) i que inclogui la data concreta i la hora

                    a la que comença i la localització del event, que en aquest cas és els jardins el Hotelito.

                """

            },

        ]

        events_CANYA_club = ["una sessió de DJ", "una festa per turistes", "una nit de mojitos", "una nit de cocktails"]

        event_CANYA_club = random.choice(events_CANYA_club)

        context_generation_events_CANYA_hclub = [

            {

                "role": "user",

                "content": f"""

                    Crea un event simulat que es celebri a la vila vacacional CANYA hlclub

                    el dia {self.current_day_str}. L'event ha d'anar relacionat amb l'oci nocturn com {event_CANYA_club}.

                    Es molt important que en la descripció aparegui la adreça del CANYA hclub que és Carrer l'Hospitalet, 17, 08811 Califòrnia, Barcelona.

                    Respon únicament amb la descripcó de l'event.

                """

            },

        ]

        events_club_patinatge = ["una sessió de portes obertes per veure entrenar al equip", "una sessió de portes obertes per principiants que vulguin venir a probar per primera vegada el patinatge", "una exhibició de l'equip local"

                                "una xerrada de la famosa patinadora Carla Sanchis sobre les seves experiencies i éxits", "una exhibició pre competició de les nostres millors patinadores"]

        event_club_patinatge = random.choice(events_club_patinatge)

        context_generation_events_patinatge = [

            {

                "role": "user",

                "content": f"""

                    Crea un event simulat com {event_club_patinatge} que es celebri al club de patinatge arístic de Canyelles el dia {self.current_day_str}.

                    Es important que la descripció del event inclogui la data concreta i la hora

                    a la que comença i la localització del event, que en aquest cas és el club de patinatge arístic. Respon únicament amb la descripció de l'event.

                """

            }

        ]

        events_TARA = ["una festa electronica", "una nit electronica", "una nit blanca amb DJ", "una festa hawaiana", "una festa organitzada per el propietaris de la vila"

                    "una festa de entrada lliure", "una nit de cocktails amb els millors baristes"]

        event_TARA = random.choice(events_TARA)




        context_generation_events_CANYA_club = [

            {

                "role": "user",

                "content": f"""

                    Crea un event simulat que es celebri a la vila vacacional TARA club

                    el dia {self.current_day_str}. L'event ha d'anar relacionat amb l'oci nocturn com {event_TARA}.

                    Es molt important que en la descripció aparegui la adreça del TARA club que és Carrer Nicaragua, 21, 08811 Canyelles, Barcelona.

                    Respon únicament amb la descripcó de l'event.

                """

            },

        ]

        events_castell = ["una nit de portes obertes al castell", "un dia de portes obertes al castell", "una visita guiada", "una visita a les masmorres del castell", "una visita a la torre del castell per veure la posta de sol"

                        "una conferencia sobre la historia de Canyelles", "una nit de histories de terror pels més petits"]

        event_castell = random.choice(events_castell)

        context_generation_events_castell = [

            {

                "role": "user",

                "content": f"""

                    Crea un event simulat que es celebri al castell de Canyelles el dia {self.current_day_str} com {event_castell}.

                    Es important que la descripció del event sigui molt extensa i que inclogui la data i la hora

                    a la que comença i la localització del event, que en aquest cas és el castell de Canyelles.

                """

            }

        ]


        self.list_of_contexts = [

            context_generation_night_events,

            context_generation_sports_events_pavello,

            context_generation_sports_events_futbol_field1,

            context_generation_sports_events_futbol_field2,

            context_generation_events_school,

            context_generation_events_hotelito,

            context_generation_events_hotelito_night,

            context_generation_events_CANYA_hclub,

            context_generation_events_patinatge,

            context_generation_events_CANYA_club,

            context_generation_events_castell,

        ]


    def generate_events(self,):
        # Add the minus 4 so we do not get 11 events for some days
        number_of_events = random.randint(1, len(self.list_of_contexts) - 4)

        # Get a random list len=number_of_elements from the list of contexts:
        today_contexts_events = random.sample(self.list_of_contexts, number_of_events)

        print("\nGenerating events..\n")
        for event_context in today_contexts_events:
            completion = openai.ChatCompletion.create(
                deployment_id=self.deployment_name,
                messages=event_context
            )

            reply_content = completion.choices[0].message.content
            self.descriptions_events_today.append(reply_content)
        


def main():
    event_generator = EventGenerator()
    event_generator.generate_events()
    
    print(len(event_generator.descriptions_events_today))
    print(event_generator.descriptions_events_today)

main()