"""Small bilingual text helpers for Streamlit views."""

import html

import streamlit as st


def bilingual_page_title(english: str, chinese: str, icon: str = "") -> None:
    icon_html = f"{icon} " if icon else ""
    st.markdown(
        f"""
        <div class="bilingual-title">
            <h1>{icon_html}{html.escape(english)}</h1>
            <p>{html.escape(chinese)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def bilingual_section(english: str, chinese: str, icon: str = "") -> None:
    icon_html = f"{icon} " if icon else ""
    st.markdown(
        f"""
        <div class="bilingual-section">
            <h3>{icon_html}{html.escape(english)}</h3>
            <p>{html.escape(chinese)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def bilingual_chart_title(index: str, english: str, chinese: str, icon: str = "") -> None:
    icon_html = f"{icon} " if icon else ""
    prefix = f"{html.escape(index)}. " if index else ""
    st.markdown(
        f"""
        <div class="bilingual-chart-title">
            <h4>{prefix}{icon_html}{html.escape(english)}</h4>
            <p>{html.escape(chinese)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def bilingual_sidebar_title(english: str, chinese: str, icon: str = "") -> None:
    icon_html = f"{icon} " if icon else ""
    st.sidebar.markdown(
        f"""
        <div class="bilingual-sidebar-title">
            <h2>{icon_html}{html.escape(english)}</h2>
            <p>{html.escape(chinese)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def bilingual_sidebar_section(english: str, chinese: str, icon: str = "") -> None:
    icon_html = f"{icon} " if icon else ""
    st.sidebar.markdown(
        f"""
        <div class="bilingual-sidebar-section">
            <h3>{icon_html}{html.escape(english)}</h3>
            <p>{html.escape(chinese)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
