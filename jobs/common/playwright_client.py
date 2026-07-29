from playwright.sync_api import sync_playwright
from jobs.common.logger import logger

class PlaywrightBrowserSession:
    """Gestionnaire de session de navigation unique pour le scraping dynamique."""

    def __init__(self) -> None:
        self._playwright = None
        self.browser = None
        self.context = None

    def __enter__(self):
        """Démarre le navigateur unique lors de l'entrée dans le bloc contextuel."""
        logger.info("Initialisation du moteur Playwright unique pour la session...")
        self._playwright = sync_playwright().start()
        self.browser = self._playwright.chromium.launch(headless=True)
        # Simulation d'un navigateur standard pour éviter les blocages de sécurité
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36",
            ignore_https_errors=True
        )
        return self

    def get_page_html(self, url: str) -> str:
        """Exécute le JavaScript d'une URL et renvoie le code HTML complet après rendu."""
        page = None
        try:
            page = self.context.new_page()
            # Attente que le réseau soit calme
            page.goto(url, timeout=20000, wait_until="networkidle")
            return page.content()
        except Exception as e:
            logger.warning(f"Erreur de rendu Playwright sur {url} : {e}")
            return ""
        finally:
            if page:
                page.close() # On ferme l'onglet, mais on garde le navigateur allumé !

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ferme proprement le navigateur unique à la fin du script."""
        logger.info("Fermeture de la session globale Playwright.")
        if self.browser:
            self.browser.close()
        if self._playwright:
            self._playwright.stop()
