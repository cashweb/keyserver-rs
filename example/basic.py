import bitcoin
import os
import requests
import hashlib
from keyserver_pb2 import *
from wrapper_pb2 import *
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from bitcoin.core.key import CECKey
from bitcoin.wallet import P2PKHBitcoinAddress
from bitcoin.core import CMutableTransaction, CMutableTxIn
from decimal import Decimal
from hashlib import sha256
from paymentrequest_pb2 import *
from time import time, sleep

# Generate protobuf code
# protoc -I=../src/proto --python_out=. ../src/proto/*.proto

# Run bitcoind in regtest mode
# bitcoind -daemon -regtest -zmqpubrawtx=tcp://127.0.0.1:28332 -rpcallowip=0.0.0.0/0 -server  \
# -rpcuser=username -rpcpassword=password
# bitcoin-cli -regtest -rpcuser=username -rpcpassword=password generate 101

# Run the keyserver
# cargo run

BASE_URL = "http://0.0.0.0:8080"
bitcoin.SelectParams("regtest")

# Init Bitcoin RPC
rpc_user = "user"
rpc_password = "password"
rpc_connection = AuthServiceProxy(
    "http://%s:%s@127.0.0.1:18443" % (rpc_user, rpc_password))

# Generate keys
secret = os.urandom(16)
keypair = CECKey()
keypair.set_compressed(True)
keypair.set_secretbytes(secret)
private_key = keypair.get_privkey()
public_key = keypair.get_pubkey()

# Generate key addr
key_addr = str(P2PKHBitcoinAddress.from_pubkey(public_key))

# Construct Payload
header = Header(name="Something wicked", value="this way comes")
entry = Entry(
    headers=[header], entry_data=b'This gonna be so fucking fast')
timestamp = int(time())
metadata = AddressMetadata(timestamp=timestamp, ttl=3000, entries=[entry])

# Sign
raw_metadata = metadata.SerializeToString()
digest = sha256(raw_metadata).digest()
signature, _ = keypair.sign_compact(digest)

# Address metadata
auth_wrapper = AuthWrapper(
    pub_key=public_key, serialized_payload=raw_metadata, scheme=1, signature=signature)
raw_addr_meta = auth_wrapper.SerializeToString()

# Commit
metadata_digest = sha256(raw_addr_meta).digest().hex()
response = requests.post(url=BASE_URL + "/commit", params={
    'address': key_addr,
    'metadata_digest': metadata_digest
})
assert(response.status_code == 402)  # Payment required

# Deserialize invoice
payment_request = PaymentRequest.FromString(response.content)
payment_details_raw = payment_request.serialized_payment_details
payment_details = PaymentDetails.FromString(payment_details_raw)

# Payment amount
burn_amount = Decimal(100_000) / 1_00_000_000

# Collect inputs
fee = Decimal(5) / 10_000_000
utxos = rpc_connection.listunspent()
inputs = []
input_value = Decimal(0)
for utxo in utxos:
    if input_value < burn_amount + fee:
        inputs.append({
            "txid": utxo["txid"],
            "vout": utxo["vout"]
        })
        input_value += utxo["amount"]
    else:
        break

# Create outputs
my_addr = utxo["address"]
change = input_value - burn_amount - fee
op_return = payment_details.outputs[0].script[2:].hex()
outputs = [
    {
        "data": op_return
    },
    {
        my_addr: change  # Change output
    }
]

# Create tx
raw_tx_unsigned = rpc_connection.createrawtransaction(inputs, outputs)
signed_raw_tx = bytes.fromhex(
    rpc_connection.signrawtransactionwithwallet(raw_tx_unsigned)["hex"])

# Construct payment message
payment = Payment(merchant_data=payment_details.merchant_data,
                  transactions=[signed_raw_tx])
payment_raw = payment.SerializeToString()

# Send payment
payment_url = BASE_URL + payment_details.payment_url
headers = {
    "Content-Type": "application/bitcoincash-payment",
    "Accept": "application/bitcoincash-paymentack"
}
response = requests.post(url=payment_url, data=payment_raw,
                         headers=headers, allow_redirects=False)
payment_ack = PaymentACK.FromString(response.content)
print("PaymentACK memo:", payment_ack.memo)

# Token URL for PUT
redirect_url = response.headers["Location"]
token_header = response.headers["Authorization"]

# Put metadata using payment token
response = requests.put(url=BASE_URL + redirect_url, data=raw_addr_meta, headers={
                        "Authorization": token_header})

sleep(3)

# Get metadata
response = requests.get(url=BASE_URL + "/keys/" + key_addr)
addr_metadata = AuthWrapper.FromString(response.content)
print(addr_metadata)
