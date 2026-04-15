from app.core.config import Settings


class OCIChatModelFactory:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build(self):
        if not self.settings.oci_ready:
            return None

        try:
            from langchain_oci.chat_models import ChatOCIGenAI
        except ImportError:
            return None

        return ChatOCIGenAI(
            model_id=self.settings.oci_model_id,
            service_endpoint=self.settings.oci_service_endpoint,
            compartment_id=self.settings.oci_compartment_id,
            auth_profile=self.settings.oci_auth_profile,
            auth_file_location=self.settings.oci_config_file,
            model_kwargs={"temperature": 0},
        )

