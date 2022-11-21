from omegaconf import DictConfig
import streamlit as st


def create_dashboards(config: DictConfig):
    """
    Streamlit app creation
    """
    st.markdown("# Crime dashboard")
