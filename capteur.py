import json
import datetime as dt
import random
import time
import uuid
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic())

def generate_transaction():
    transaction_types = ['achat', 'remboursement', 'transfert']
    payment_methods = ['carte_de_credit', 'especes', 'virement_bancaire', 'erreur']
    current_time = dt.datetime.now().isoformat()
    cities = ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims", "Le Havre", "Saint-Étienne", "Toulon", None]
    streets = ["Rue de la République", "Rue de Paris", "rue Auguste Delaune", "Rue Gustave Courbet", "Rue de Luxembourg", "Rue Fontaine", "Rue Zinedine Zidane", "Rue de Bretagne", "Rue Marceaux", "Rue Gambetta", "Rue du Faubourg Saint-Antoine", "Rue de la Grande Armée", "Rue of the Villette", "Rue of the Pump", "Rue Saint-Michel", None]
    transaction_data = {
        "id_transaction": str(uuid.uuid4()),
        "type_transaction": random.choice(transaction_types),
        "montant": round(random.uniform(10.0, 1000.0), 2),
        "devise": "USD",
        "date": current_time,
        "lieu": f"{random.choice(cities)}, {random.choice(streets)}" if random.choice([True, False]) else None,
        "moyen_paiement": random.choice(payment_methods),
        "details": {
            "produit": f"Produit{random.randint(1, 100)}",
            "quantite": random.randint(1, 10),
            "prix_unitaire": round(random.uniform(5.0, 200.0), 2)
        },
        "utilisateur": {
            "id_utilisateur": f"User{random.randint(1, 1000)}",
            "nom": f"Utilisateur{random.randint(1, 1000)}",
            "adresse": f"{random.randint(1, 1000)} {random.choice(streets)}, {random.choice(cities)}",
            "email": f"utilisateur{random.randint(1, 1000)}@example.com"
        }
    }
    return transaction_data

def main():
    p = Producer({'bootstrap.servers': '127.0.0.1:9092'})
    for _ in range(200):
        data = json.dumps(generate_transaction()).encode('utf-8')
        p.produce('transaction', data, callback=delivery_report)
        p.poll(0.5)
    p.flush()

if __name__ == "__main__":
    main()
