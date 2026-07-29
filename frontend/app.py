import streamlit as st
import requests
import pandas as pd

# Configuration

st.set_page_config(
    page_title="University Data Platform",
    page_icon="🎓",
    layout="wide"
)

API_URL = "http://localhost:8000/search"

# Style

st.markdown(
    """
    <style>
    .title{
        text-align:center;
        font-size:40px;
        font-weight:bold;
        color:#1f77b4;
    }

    .subtitle{
        text-align:center;
        font-size:18px;
        color:gray;
        margin-bottom:30px;
    }

    .card{
        border:1px solid #DDDDDD;
        border-radius:10px;
        padding:15px;
        margin-top:15px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<p class="title">🎓 University Data Platform</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="subtitle">Recherche de publications scientifiques</p>',
    unsafe_allow_html=True,
)

st.markdown("---")

# Barre de recherche

query = st.text_input(
    "",
    placeholder="Tapez un mot-clé puis appuyez sur Entrée..."
)

# Recherche

if query.strip():

    with st.spinner("Recherche..."):

        try:

            response = requests.get(
                API_URL,
                params={"q": query},
                timeout=20,
            )

            if response.status_code != 200:
                st.error(response.text)
                st.stop()

            results = response.json()

        except Exception as e:
            st.error("Impossible de joindre l'API FastAPI.")
            st.exception(e)
            st.stop()

    st.markdown("---")

    st.success(f"{len(results)} résultat(s) trouvé(s).")

    if len(results) == 0:
        st.info("Aucune publication trouvée.")
        st.stop()

    rows = []

    for hit in results:

        src = hit["_source"]

        rows.append(
            {
                "Titre": src.get("title", ""),
                "Année": src.get("publication_year", ""),
                "Journal": src.get("journal_name", ""),
                "DOI": src.get("doi", ""),
            }
        )

    df = pd.DataFrame(rows)

    st.dataframe(df)

    st.markdown("---")

    st.subheader("Détails")

    for i, hit in enumerate(results, start=1):

        src = hit["_source"]

        with st.expander(f"{i}. {src.get('title','Sans titre')}"):

            st.write("### Informations")

            st.write("**Titre :**", src.get("title", ""))

            st.write("**Année :**", src.get("publication_year", ""))

            st.write("**Journal :**", src.get("journal_name", ""))

            st.write("**DOI :**", src.get("doi", ""))

            st.write("**Langue :**", src.get("language", ""))

            authors = src.get("authors", [])

            if isinstance(authors, list):

                st.write("**Auteurs :**")

                for author in authors:
                    st.write("- " + author)

            else:
                st.write("**Auteurs :**", authors)