use std::ptr::NonNull;

#[derive(Copy, Clone)]
pub struct DataPtr(NonNull<u64>);

impl<'a> DataPtr {
    pub fn new(reference: &'a u64) -> DataPtr {
        DataPtr(NonNull::from(reference))
    }
}

unsafe impl Send for DataPtr {}

impl AsRef<u64> for DataPtr {
    fn as_ref(&self) -> &u64 {
        unsafe { self.0.as_ref() }
    }
}

impl AsMut<u64> for DataPtr {
    fn as_mut(&mut self) -> &mut u64 {
        unsafe { self.0.as_mut() }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn data_ptr() {
        let data: u64 = 0;
        let mut ptr = DataPtr::new(&data);
        *ptr.as_mut() = 42;
        assert_eq!(*ptr.as_ref(), 42);
    }
}
