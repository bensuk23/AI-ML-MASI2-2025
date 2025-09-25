# MyAnimeList Dataset Creation Pipeline
# Script complet pour scraper MAL + enrichissement NLP

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from textblob import TextBlob
import numpy as np
from datetime import datetime
import random
from urllib.parse import urljoin

class MALDatasetCreator:
    def __init__(self, delay=1.0):
        """
        Créateur de dataset MyAnimeList enrichi
        delay: délai entre requêtes (respecter le site)
        """
        self.base_url = "https://myanimelist.net"
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def get_top_anime_list(self, limit=1500):
        """Récupère la liste des top animes MAL"""
        animes = []
        page = 0
        
        while len(animes) < limit:
            url = f"{self.base_url}/topanime.php?limit={page * 50}"
            
            try:
                response = self.session.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Parser la liste des animes
                ranking_items = soup.find_all('tr', class_='ranking-list')
                
                if not ranking_items:
                    break
                    
                for item in ranking_items:
                    try:
                        # Extraire l'ID et titre
                        link = item.find('a', class_='hoverinfo_trigger')
                        if link:
                            anime_url = link['href']
                            anime_id = int(re.search(r'/anime/(\d+)/', anime_url).group(1))
                            title = link.text.strip()
                            
                            # Score et autres infos de base
                            score_elem = item.find('span', class_='score')
                            score = float(score_elem.text) if score_elem and score_elem.text != 'N/A' else None
                            
                            animes.append({
                                'anime_id': anime_id,
                                'title': title,
                                'score': score,
                                'url': anime_url
                            })
                            
                    except Exception as e:
                        print(f"Erreur parsing item: {e}")
                        continue
                        
                page += 1
                print(f"Page {page} récupérée, {len(animes)} animes trouvés")
                time.sleep(self.delay)
                
            except Exception as e:
                print(f"Erreur page {page}: {e}")
                break
                
        return animes[:limit]
    
    def scrape_anime_details(self, anime_id):
        """Scrape les détails d'un anime spécifique"""
        url = f"{self.base_url}/anime/{anime_id}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            details = {}
            
            # Synopsis
            synopsis_elem = soup.find('p', {'itemprop': 'description'})
            synopsis = synopsis_elem.text.strip() if synopsis_elem else ""
            details['synopsis'] = synopsis
            details['synopsis_length'] = len(synopsis)
            
            # Informations de base depuis le sidebar
            stats_div = soup.find('div', class_='statistics-info')
            if stats_div:
                # Membres, popularité, etc.
                for div in stats_div.find_all('div', class_='spaceit_pad'):
                    text = div.get_text()
                    if 'Members:' in text:
                        members = re.search(r'Members:\s*([\d,]+)', text)
                        details['members'] = int(members.group(1).replace(',', '')) if members else 0
                    elif 'Popularity:' in text:
                        pop = re.search(r'Popularity:\s*#(\d+)', text)
                        details['popularity'] = int(pop.group(1)) if pop else None
                    elif 'Ranked:' in text:
                        rank = re.search(r'Ranked:\s*#(\d+)', text)
                        details['rank'] = int(rank.group(1)) if rank else None
            
            # Genres
            genre_links = soup.find_all('span', {'itemprop': 'genre'})
            genres = [genre.text.strip() for genre in genre_links]
            details['genres'] = ', '.join(genres) if genres else ""
            
            # Studio et source
            info_divs = soup.find_all('div', class_='spaceit_pad')
            for div in info_divs:
                text = div.get_text()
                if 'Studios:' in text:
                    studio_links = div.find_all('a')
                    studios = [link.text.strip() for link in studio_links]
                    details['studios'] = ', '.join(studios) if studios else ""
                elif 'Source:' in text:
                    details['source'] = text.split('Source:')[1].strip() if 'Source:' in text else ""
            
            return details
            
        except Exception as e:
            print(f"Erreur scraping anime {anime_id}: {e}")
            return {}
    
    def scrape_reviews(self, anime_id, limit=50):
        """Scrape les reviews d'un anime"""
        reviews = []
        url = f"{self.base_url}/anime/{anime_id}/reviews"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Trouver les reviews
            review_divs = soup.find_all('div', class_='review-element')[:limit]
            
            for review_div in review_divs:
                try:
                    # Texte de la review
                    text_div = review_div.find('div', class_='text')
                    if text_div:
                        review_text = text_div.get_text(strip=True)
                        
                        # Score utilisateur si disponible
                        score_div = review_div.find('div', class_='rating')
                        user_score = None
                        if score_div:
                            score_match = re.search(r'(\d+)', score_div.text)
                            user_score = int(score_match.group(1)) if score_match else None
                        
                        reviews.append({
                            'text': review_text,
                            'user_score': user_score
                        })
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"Erreur scraping reviews {anime_id}: {e}")
            
        return reviews
    
    def analyze_sentiment(self, text):
        """Analyse de sentiment avec TextBlob"""
        if not text:
            return 0.0, 0.0
            
        try:
            blob = TextBlob(text)
            return blob.sentiment.polarity, blob.sentiment.subjectivity
        except:
            return 0.0, 0.0
    
    def extract_emotional_keywords(self, text):
        """Extrait des mots-clés émotionnels"""
        positive_words = ['amazing', 'excellent', 'fantastic', 'love', 'perfect', 
                         'brilliant', 'outstanding', 'masterpiece', 'incredible']
        negative_words = ['terrible', 'awful', 'boring', 'disappointing', 'worst',
                         'horrible', 'stupid', 'waste', 'pathetic']
        
        text_lower = text.lower()
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        return pos_count, neg_count
    
    def create_enriched_dataset(self, anime_limit=1000, reviews_per_anime=30):
        """Création du dataset complet enrichi"""
        
        print("🚀 Début de la création du dataset enrichi...")
        
        # 1. Récupération des top animes
        print("📋 Récupération de la liste des animes...")
        animes = self.get_top_anime_list(anime_limit)
        
        # 2. Enrichissement avec détails et reviews
        enriched_data = []
        
        for i, anime in enumerate(animes):
            print(f"📊 Processing {i+1}/{len(animes)}: {anime['title']}")
            
            # Détails de base
            details = self.scrape_anime_details(anime['anime_id'])
            
            # Reviews et analyse NLP
            reviews = self.scrape_reviews(anime['anime_id'], reviews_per_anime)
            
            # Analyse des reviews
            if reviews:
                all_review_text = ' '.join([r['text'] for r in reviews])
                
                # Sentiment analysis
                polarity, subjectivity = self.analyze_sentiment(all_review_text)
                
                # Mots-clés émotionnels
                pos_keywords, neg_keywords = self.extract_emotional_keywords(all_review_text)
                
                # Calcul de controverse (variance des scores utilisateurs)
                user_scores = [r['user_score'] for r in reviews if r['user_score']]
                controversy = np.std(user_scores) if len(user_scores) > 1 else 0
                
                # Ratio hype (différence entre score MAL et sentiment reviews)
                hype_ratio = anime['score'] - polarity if anime['score'] and polarity else 0
                
            else:
                polarity = subjectivity = pos_keywords = neg_keywords = 0
                controversy = hype_ratio = 0
            
            # Construction de l'entrée finale
            entry = {
                # Colonnes originales
                'anime_id': anime['anime_id'],
                'title': anime['title'],
                'score': anime['score'],
                'rank': details.get('rank'),
                'popularity': details.get('popularity'),
                'members': details.get('members'),
                'genres': details.get('genres', ''),
                'studios': details.get('studios', ''),
                
                # Colonnes CRÉÉES (nouvelles features)
                'sentiment_score': round(polarity, 3),
                'controversy_index': round(controversy, 3),
                'emotional_intensity': round(subjectivity, 3),
                'review_count_scraped': len(reviews),
                'synopsis_length': details.get('synopsis_length', 0),
                'positive_keywords_count': pos_keywords,
                'negative_keywords_count': neg_keywords,
                'hype_ratio': round(hype_ratio, 3)
            }
            
            enriched_data.append(entry)
            
            # Pause respectueuse
            time.sleep(self.delay + random.uniform(0.5, 1.5))
        
        # 3. Création du DataFrame final
        df = pd.DataFrame(enriched_data)
        
        print(f"✅ Dataset créé avec {len(df)} animes et {len(df.columns)} colonnes")
        print(f"📊 Colonnes: {list(df.columns)}")
        
        return df

# Utilisation du script
def main():
    """Fonction principale pour créer le dataset"""
    
    # Créer l'instance du scraper
    creator = MALDatasetCreator(delay=1.5)  # 1.5s entre requêtes
    
    # Créer le dataset enrichi
    # ATTENTION: Commencez petit pour tester (50-100 animes)
    df = creator.create_enriched_dataset(anime_limit=100, reviews_per_anime=20)
    
    # Sauvegarder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"mal_enriched_dataset_{timestamp}.csv"
    df.to_csv(filename, index=False, encoding='utf-8')
    
    print(f"💾 Dataset sauvegardé: {filename}")
    
    # Aperçu du dataset
    print("\n📋 Aperçu du dataset créé:")
    print(df.head())
    print(f"\n📊 Shape: {df.shape}")
    print(f"\n📈 Colonnes créées par votre pipeline:")
    created_cols = ['sentiment_score', 'controversy_index', 'emotional_intensity', 
                   'review_count_scraped', 'synopsis_length', 'positive_keywords_count',
                   'negative_keywords_count', 'hype_ratio']
    for col in created_cols:
        if col in df.columns:
            print(f"  - {col}: {df[col].describe()['mean']:.3f} (moyenne)")

if __name__ == "__main__":
    # Installation des dépendances nécessaires
    print("📦 Assurez-vous d'avoir installé les dépendances:")
    print("pip install requests beautifulsoup4 pandas textblob numpy")
    print("python -m textblob.corpora.download_lite")
    print()
    
    main()