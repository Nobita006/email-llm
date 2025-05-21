import streamlit as st
import boto3
import json
from datetime import datetime

# --- AWS Bedrock Configuration ---
BEDROCK_REGION = "us-east-1"
# ----> 1. PUT THE CORRECT KNOWLEDGE BASE ID HERE <----
#         Get this from your AWS Bedrock Console.
#         Based on your previous screenshot, it should be BWXXMZEXGU.
#         The LATEST ERROR MESSAGE showed BWXXMZEXGU. You need to find the *actual, current* ID.
KNOWLEDGE_BASE_ID = "BWXXMZEXGU" 

# ----> 2. USE THE CORRECT AND AVAILABLE CLAUDE 3 SONNET MODEL ID <----
MODEL_ID = "anthropic.claude-3-7-sonnet-20250219-v1:0" 

# Construct the full Model ARN
MODEL_ARN = f"arn:aws:bedrock:{BEDROCK_REGION}::foundation-model/{MODEL_ID}"

# Initialize Boto3 Bedrock Agent Runtime client
try:
    bedrock_agent_runtime_client = boto3.client(
        "bedrock-agent-runtime",
        region_name=BEDROCK_REGION
    )
    st.sidebar.success(f"Bedrock client initialized for region: {BEDROCK_REGION}")
except Exception as e:
    st.error(f"Error initializing Bedrock Agent Runtime client: {e}")
    st.sidebar.error(f"Bedrock client init FAILED: {e}")
    st.stop()

# --- Streamlit App Interface ---
st.title("📧 Email Knowledge Base Q&A")
st.markdown(f"""
Ask questions about your indexed emails.
- **Knowledge Base ID:** `{KNOWLEDGE_BASE_ID}`
- **Generation Model:** `{MODEL_ID}`
""")

# ... (the rest of the Streamlit app code remains the same as the last complete version I gave you) ...
# (Make sure the rest of the code for displaying messages, citations, and the 
# bedrock_agent_runtime_client.retrieve_and_generate call is present)

# Initialize chat history in session state
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message["role"] == "assistant" and "citations" in message and message["citations"]:
            with st.expander("📚 View Sources", expanded=False):
                for i, citation in enumerate(message["citations"]):
                    st.markdown(f"---") 
                    if citation.get('retrievedReferences'):
                        for ref_idx, ref in enumerate(citation.get('retrievedReferences', [])):
                            st.markdown(f"**Reference {i+1}.{ref_idx+1}:**")
                            if ref.get('location', {}).get('s3Location', {}).get('uri'):
                                st.markdown(f"- S3 URI: `{ref['location']['s3Location']['uri']}`")
                            if ref.get('content', {}).get('text'):
                                text_area_key = f"cite_{message.get('timestamp', datetime.now().timestamp())}_{i}_{ref_idx}"
                                st.text_area(f"Retrieved Content Snippet:", 
                                             value=ref['content']['text'][:1000]+"..." if len(ref['content']['text']) > 1000 else ref['content']['text'], 
                                             height=150, 
                                             key=text_area_key,
                                             disabled=True)
                    else:
                        st.write("No detailed references found for this citation segment.")

if prompt := st.chat_input("Ask a question about your emails..."):
    current_timestamp = datetime.now().timestamp()
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({
        "role": "user", 
        "content": prompt, 
        "timestamp": current_timestamp
    })

    try:
        with st.spinner("Searching emails and generating answer..."):
            response = bedrock_agent_runtime_client.retrieve_and_generate(
                input={'text': prompt},
                retrieveAndGenerateConfiguration={
                    'type': 'KNOWLEDGE_BASE',
                    'knowledgeBaseConfiguration': {
                        'knowledgeBaseId': KNOWLEDGE_BASE_ID,
                        'modelArn': MODEL_ARN
                    }
                }
            )
        assistant_response_text = response.get('output', {}).get('text', "Sorry, I couldn't retrieve an answer or the answer was empty.")
        citations = response.get('citations', [])
        with st.chat_message("assistant"):
            st.markdown(assistant_response_text)
            if citations:
                with st.expander("📚 View Sources", expanded=True):
                    for i, citation in enumerate(citations):
                        st.markdown(f"---")
                        if citation.get('retrievedReferences'):
                            for ref_idx, ref in enumerate(citation.get('retrievedReferences', [])):
                                st.markdown(f"**Reference {i+1}.{ref_idx+1}:**")
                                if ref.get('location', {}).get('s3Location', {}).get('uri'):
                                    st.markdown(f"- S3 URI: `{ref['location']['s3Location']['uri']}`")
                                if ref.get('content', {}).get('text'):
                                    text_area_key_new = f"new_cite_{current_timestamp}_{i}_{ref_idx}"
                                    st.text_area(f"Retrieved Content Snippet:", 
                                                 value=ref['content']['text'][:1000]+"..." if len(ref['content']['text']) > 1000 else ref['content']['text'], 
                                                 height=150, 
                                                 key=text_area_key_new,
                                                 disabled=True)
                        else:
                             st.write("No detailed references found for this citation segment.")
            elif not assistant_response_text:
                 st.markdown("I found some information, but couldn't formulate a direct answer. You might want to check the sources if any were retrieved, or try rephrasing your question.")
        st.session_state.messages.append({
            "role": "assistant",
            "content": assistant_response_text,
            "citations": citations,
            "timestamp": current_timestamp
        })
    except Exception as e:
        error_message_full = f"Error querying Knowledge Base: {str(e)}"
        st.error(error_message_full)
        st.session_state.messages.append({
            "role": "assistant", 
            "content": f"An error occurred: {str(e)}",
            "citations": [],
            "timestamp": current_timestamp
        })
        print(f"Full error details: {error_message_full}")

st.sidebar.header("About")
st.sidebar.info(
    "This app uses Amazon Bedrock with a Knowledge Base (powered by Kendra GenAI Index) "
    "to answer questions based on your indexed email content. "
    f"The generation is performed by the {MODEL_ID} model."
)