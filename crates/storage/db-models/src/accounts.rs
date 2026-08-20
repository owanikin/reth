use alloy_primitives::{Address, B256};
use reth_primitives_traits::{Account, ValueWithSubKey};

/// Account as it is saved in the database.
///
/// [`Address`] is the subkey.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(any(test, feature = "reth-codec"), reth_codecs::add_arbitrary_tests(compact))]
pub struct AccountBeforeTx {
    /// Address for the account. Acts as `DupSort::SubKey`.
    pub address: Address,
    /// Account state before the transaction.
    pub info: Option<Account>,
}

impl ValueWithSubKey for AccountBeforeTx {
    type SubKey = Address;

    fn get_subkey(&self) -> Self::SubKey {
        self.address
    }
}

// NOTE: Removing reth_codec and manually encode subkey
// and compress second part of the value. If we have compression
// over whole value (Even SubKey) that would mess up fetching of values with seek_by_key_subkey
#[cfg(any(test, feature = "reth-codec"))]
impl reth_codecs::Compact for AccountBeforeTx {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        // for now put full bytes and later compress it.
        buf.put_slice(self.address.as_slice());

        let acc_len = if let Some(account) = self.info { account.to_compact(buf) } else { 0 };
        acc_len + 20
    }

    fn from_compact(mut buf: &[u8], len: usize) -> (Self, &[u8]) {
        use bytes::Buf;
        let address = Address::from_slice(&buf[..20]);
        buf.advance(20);

        let info = (len - 20 > 0).then(|| {
            let (acc, advanced_buf) = Account::from_compact(buf, len - 20);
            buf = advanced_buf;
            acc
        });

        (Self { address, info }, buf)
    }
}

#[cfg(any(test, feature = "reth-codec"))]
reth_codecs::impl_compression_for_compact!(AccountBeforeTx);

/// Partial-state account and storage-root commitments before a block transition.
///
/// [`Address`] is the subkey.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
#[cfg_attr(any(test, feature = "arbitrary"), derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(any(test, feature = "reth-codec"), reth_codecs::add_arbitrary_tests(compact))]
pub struct PartialStateAccountBefore {
    /// Address for the account. Acts as `DupSort::SubKey`.
    pub address: Address,
    /// Account commitment before the transition.
    pub account: Option<Account>,
    /// Preserved storage-root commitment before the transition.
    pub storage_root: Option<B256>,
}

impl ValueWithSubKey for PartialStateAccountBefore {
    type SubKey = Address;

    fn get_subkey(&self) -> Self::SubKey {
        self.address
    }
}

// Keep the address uncompressed at the beginning so dupsort subkey seeks remain valid.
#[cfg(any(test, feature = "reth-codec"))]
impl reth_codecs::Compact for PartialStateAccountBefore {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        const ACCOUNT_PRESENT: u8 = 1;
        const STORAGE_ROOT_PRESENT: u8 = 2;

        buf.put_slice(self.address.as_slice());
        let flags = (u8::from(self.account.is_some()) * ACCOUNT_PRESENT) |
            (u8::from(self.storage_root.is_some()) * STORAGE_ROOT_PRESENT);
        buf.put_u8(flags);

        let mut len = 21;
        if let Some(storage_root) = self.storage_root {
            buf.put_slice(storage_root.as_slice());
            len += B256::len_bytes();
        }
        if let Some(account) = self.account {
            len += account.to_compact(buf);
        }
        len
    }

    fn from_compact(mut buf: &[u8], len: usize) -> (Self, &[u8]) {
        use bytes::Buf;

        const ACCOUNT_PRESENT: u8 = 1;
        const STORAGE_ROOT_PRESENT: u8 = 2;

        let address = Address::from_slice(&buf[..20]);
        buf.advance(20);
        let flags = buf.get_u8();
        let mut consumed = 21;

        let storage_root = (flags & STORAGE_ROOT_PRESENT != 0).then(|| {
            let root = B256::from_slice(&buf[..B256::len_bytes()]);
            buf.advance(B256::len_bytes());
            consumed += B256::len_bytes();
            root
        });
        let account = (flags & ACCOUNT_PRESENT != 0).then(|| {
            let (account, remaining) = Account::from_compact(buf, len - consumed);
            buf = remaining;
            account
        });

        (Self { address, account, storage_root }, buf)
    }
}

#[cfg(any(test, feature = "reth-codec"))]
reth_codecs::impl_compression_for_compact!(PartialStateAccountBefore);
